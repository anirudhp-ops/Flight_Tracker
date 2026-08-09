import pickle
import numpy as np


class DelayPredictor:
    def __init__(self, model_path: str):
        with open(model_path, "rb") as f:
            bundle = pickle.load(f)
        self.model      = bundle["model"]
        self.le_carrier = bundle["le_carrier"]
        self.le_origin  = bundle["le_origin"]
        self.le_dest    = bundle["le_dest"]
        self.features   = bundle["features"]

    @staticmethod
    def _normalize_airport_code(code: str) -> str:
        """
        The trained model's le_origin/le_dest were fit on 3-letter IATA
        codes (e.g. "JFK") — confirmed directly against the pickled
        LabelEncoder's .classes_ (359 entries, all 3 characters, "JFK"
        present, "KJFK" absent). Every airport code this app actually
        produces is 4-letter ICAO (flight_tracker/ingestion/client.py's
        mock AIRPORTS list and its live AeroAPI parser both use
        code_icao) — without this normalization, every single prediction
        hits the unseen-label ValueError fallback in predict()/
        predict_with_confidence() below, silently returning dep_delay
        untouched and (once predict_with_confidence existed to surface it)
        confidence=0.0, on effectively 100% of real traffic. Found via
        testing predict_with_confidence against the live pipeline's actual
        codes (KJFK, KDFW, ...), not assumed.

        Mainland-US ICAO codes are, by convention, "K" + the 3-letter IATA
        code — stripping a leading "K" from a 4-letter code recovers the
        IATA form the model expects for the vast majority of real US
        airports. This is a real but partial fix: Hawaii/Alaska/territories
        (PHNL, PANC, TJSJ, ...) don't follow the K-prefix convention and
        will still fall through to the ValueError fallback — not silently
        "fixed" by this, just no longer broken for the common case.
        """
        if len(code) == 4 and code.startswith("K"):
            return code[1:]
        return code

    def _encode_features(self, airline_code, origin, destination, dep_delay, air_time, distance,
                          carrier_delay, weather_delay, nas_delay, late_aircraft_delay):
        """Raises ValueError on an unseen label — same as LabelEncoder.transform()."""
        origin = self._normalize_airport_code(origin)
        destination = self._normalize_airport_code(destination)
        carrier_enc = self.le_carrier.transform([airline_code])[0]
        origin_enc  = self.le_origin.transform([origin])[0]
        dest_enc    = self.le_dest.transform([destination])[0]
        return np.array([[
            carrier_enc, origin_enc, dest_enc,
            dep_delay, air_time, distance,
            carrier_delay, weather_delay, nas_delay, late_aircraft_delay
        ]])

    def predict(self, airline_code: str, origin: str, destination: str,
                dep_delay: float, air_time: float, distance: float,
                carrier_delay: float = 0, weather_delay: float = 0,
                nas_delay: float = 0, late_aircraft_delay: float = 0) -> float:
        try:
            X = self._encode_features(
                airline_code, origin, destination, dep_delay, air_time, distance,
                carrier_delay, weather_delay, nas_delay, late_aircraft_delay,
            )
        except ValueError:
            # unseen label — return dep_delay as fallback
            return dep_delay

        return float(self.model.predict(X)[0])

    def predict_with_confidence(self, airline_code: str, origin: str, destination: str,
                                 dep_delay: float, air_time: float, distance: float,
                                 carrier_delay: float = 0, weather_delay: float = 0,
                                 nas_delay: float = 0, late_aircraft_delay: float = 0) -> tuple[float, float]:
        """
        Returns (predicted_delay_minutes, model_confidence in [0, 1]).

        Confidence here is NOT the model's R² — R² is a fixed property of
        the trained model as a whole, evaluated once against a held-out
        test set at training time (see ml/train.py's printed MAE/R²), the
        same number for every prediction, which makes it a poor fit for a
        per-prediction PredictionEvent.model_confidence field. Instead this
        is a real per-prediction signal: the model is a
        RandomForestRegressor, an ensemble of independently-trained trees
        that each vote on the same input — asking each tree individually
        for its prediction (self.model.estimators_) and looking at how
        much they agree is a standard, legitimate RandomForest uncertainty
        heuristic. Tight agreement (low spread relative to the prediction's
        own magnitude) means high confidence; wide disagreement between
        trees means the model itself isn't sure, and confidence reflects
        that.

        On an unseen label, matches predict()'s fallback (returns
        dep_delay) but with confidence=0.0 — that value isn't a model
        prediction at all, so it shouldn't claim any model confidence.
        """
        try:
            X = self._encode_features(
                airline_code, origin, destination, dep_delay, air_time, distance,
                carrier_delay, weather_delay, nas_delay, late_aircraft_delay,
            )
        except ValueError:
            return dep_delay, 0.0

        tree_predictions = np.array([tree.predict(X)[0] for tree in self.model.estimators_])
        mean_pred = float(tree_predictions.mean())
        std_pred = float(tree_predictions.std())

        # +1.0 in the denominator: without it, a near-zero-delay prediction
        # with even a small absolute std gets a wildly deflated confidence
        # purely from dividing by a near-zero mean, not from real tree
        # disagreement.
        relative_spread = std_pred / (abs(mean_pred) + 1.0)
        confidence = max(0.0, min(1.0, 1.0 - relative_spread))

        return mean_pred, confidence
