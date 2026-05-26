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

    def predict(self, airline_code: str, origin: str, destination: str,
                dep_delay: float, air_time: float, distance: float,
                carrier_delay: float = 0, weather_delay: float = 0,
                nas_delay: float = 0, late_aircraft_delay: float = 0) -> float:
        try:
            carrier_enc = self.le_carrier.transform([airline_code])[0]
            origin_enc  = self.le_origin.transform([origin])[0]
            dest_enc    = self.le_dest.transform([destination])[0]
        except ValueError:
            # unseen label — return dep_delay as fallback
            return dep_delay

        X = np.array([[
            carrier_enc, origin_enc, dest_enc,
            dep_delay, air_time, distance,
            carrier_delay, weather_delay, nas_delay, late_aircraft_delay
        ]])

        return float(self.model.predict(X)[0])