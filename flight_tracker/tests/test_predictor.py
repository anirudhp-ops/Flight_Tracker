"""
Unit tests for ml/predictor.py (DelayPredictor). Loads the real trained
model (ml/model.pkl) — matches this project's "no mocks" testing
convention (see test_workers.py) and there's no meaningful way to fake a
RandomForestRegressor's tree-agreement behavior anyway. The label-encoder
classes actually in the pickle (not guessed IATA codes) drive the
known-input assertions, so this test can't drift out of sync with
whatever CSV the model was last trained on.

Run: pytest flight_tracker/tests/test_predictor.py -v
"""
from pathlib import Path

import pytest

from ml.predictor import DelayPredictor

MODEL_PATH = str(Path(__file__).resolve().parent.parent.parent / "ml" / "model.pkl")


@pytest.fixture(scope="module")
def predictor():
    return DelayPredictor(MODEL_PATH)


@pytest.fixture(scope="module")
def known_inputs(predictor):
    """A real (carrier, origin, dest) triple the model was actually
    trained on, taken straight from its own label encoders."""
    return {
        "airline_code": predictor.le_carrier.classes_[0],
        "origin": predictor.le_origin.classes_[0],
        "destination": predictor.le_dest.classes_[1 if len(predictor.le_dest.classes_) > 1 else 0],
    }


# --- model loading -------------------------------------------------------------

def test_model_loads_with_expected_bundle_components(predictor):
    assert predictor.model is not None
    assert predictor.le_carrier is not None
    assert predictor.le_origin is not None
    assert predictor.le_dest is not None
    assert isinstance(predictor.features, list)


# --- predict(): known inputs ----------------------------------------------------

def test_predict_with_known_inputs_returns_a_float(predictor, known_inputs):
    result = predictor.predict(
        airline_code=known_inputs["airline_code"],
        origin=known_inputs["origin"],
        destination=known_inputs["destination"],
        dep_delay=15.0,
        air_time=120.0,
        distance=800.0,
    )
    assert isinstance(result, float)


def test_predict_zero_dep_delay_known_inputs(predictor, known_inputs):
    result = predictor.predict(
        airline_code=known_inputs["airline_code"],
        origin=known_inputs["origin"],
        destination=known_inputs["destination"],
        dep_delay=0.0,
        air_time=60.0,
        distance=300.0,
    )
    assert isinstance(result, float)


def test_predict_normalizes_icao_k_prefixed_airport_codes(predictor, known_inputs):
    """A 4-letter 'K' + IATA code (what the rest of this app always sends,
    e.g. KJFK) must resolve to the same encoding as the bare 3-letter IATA
    form the model was actually trained on — see
    DelayPredictor._normalize_airport_code's docstring."""
    origin_iata = str(known_inputs["origin"])
    if len(origin_iata) != 3:
        pytest.skip("first le_origin class isn't a 3-letter IATA code")
    icao_form = "K" + origin_iata

    result_icao = predictor.predict(
        airline_code=known_inputs["airline_code"], origin=icao_form,
        destination=known_inputs["destination"], dep_delay=10.0, air_time=100.0, distance=500.0,
    )
    result_iata = predictor.predict(
        airline_code=known_inputs["airline_code"], origin=origin_iata,
        destination=known_inputs["destination"], dep_delay=10.0, air_time=100.0, distance=500.0,
    )
    assert result_icao == result_iata


# --- predict(): unseen-label fallback -------------------------------------------

def test_predict_unseen_carrier_falls_back_to_dep_delay(predictor, known_inputs):
    result = predictor.predict(
        airline_code="ZZ-NOT-A-REAL-CARRIER",
        origin=known_inputs["origin"], destination=known_inputs["destination"],
        dep_delay=42.0, air_time=100.0, distance=500.0,
    )
    assert result == 42.0


def test_predict_unseen_origin_falls_back_to_dep_delay(predictor, known_inputs):
    result = predictor.predict(
        airline_code=known_inputs["airline_code"],
        origin="ZZZZ", destination=known_inputs["destination"],
        dep_delay=17.0, air_time=100.0, distance=500.0,
    )
    assert result == 17.0


def test_predict_unseen_destination_falls_back_to_dep_delay(predictor, known_inputs):
    result = predictor.predict(
        airline_code=known_inputs["airline_code"],
        origin=known_inputs["origin"], destination="ZZZZ",
        dep_delay=5.0, air_time=100.0, distance=500.0,
    )
    assert result == 5.0


# --- predict_with_confidence() --------------------------------------------------

def test_predict_with_confidence_known_inputs_returns_bounded_confidence(predictor, known_inputs):
    pred, confidence = predictor.predict_with_confidence(
        airline_code=known_inputs["airline_code"],
        origin=known_inputs["origin"], destination=known_inputs["destination"],
        dep_delay=10.0, air_time=90.0, distance=450.0,
    )
    assert isinstance(pred, float)
    assert 0.0 <= confidence <= 1.0


def test_predict_with_confidence_unseen_label_returns_zero_confidence(predictor, known_inputs):
    pred, confidence = predictor.predict_with_confidence(
        airline_code="ZZ-NOT-A-REAL-CARRIER",
        origin=known_inputs["origin"], destination=known_inputs["destination"],
        dep_delay=33.0, air_time=90.0, distance=450.0,
    )
    assert pred == 33.0
    assert confidence == 0.0


def test_predict_and_predict_with_confidence_agree_on_known_inputs(predictor, known_inputs):
    """predict() returns the ensemble's .predict() (numpy internal
    aggregation); predict_with_confidence() averages each tree's vote by
    hand. These are two different aggregations (numpy's built-in vs. a
    manual mean over estimators_) and are only guaranteed to be close, not
    bit-identical."""
    direct = predictor.predict(
        airline_code=known_inputs["airline_code"], origin=known_inputs["origin"],
        destination=known_inputs["destination"], dep_delay=10.0, air_time=90.0, distance=450.0,
    )
    mean_pred, _ = predictor.predict_with_confidence(
        airline_code=known_inputs["airline_code"], origin=known_inputs["origin"],
        destination=known_inputs["destination"], dep_delay=10.0, air_time=90.0, distance=450.0,
    )
    assert abs(direct - mean_pred) < 1.0


# --- error handling: bad / missing values ----------------------------------------

def test_predict_with_none_dep_delay_does_not_crash(predictor, known_inputs):
    """Verified against the real model, not assumed: np.array([[...,None,...]])
    builds an object-dtype row, and RandomForestRegressor.predict() still
    accepts it (numeric coercion happens internally) rather than raising —
    so this asserts the actual observed behavior (a float result), not a
    TypeError that never actually occurs."""
    result = predictor.predict(
        airline_code=known_inputs["airline_code"], origin=known_inputs["origin"],
        destination=known_inputs["destination"], dep_delay=None, air_time=90.0, distance=450.0,
    )
    assert isinstance(result, float)


def test_predict_with_missing_required_kwarg_raises_typeerror(predictor, known_inputs):
    with pytest.raises(TypeError):
        predictor.predict(
            airline_code=known_inputs["airline_code"], origin=known_inputs["origin"],
            destination=known_inputs["destination"],
            # dep_delay omitted entirely — a real missing-value case
            air_time=90.0, distance=450.0,
        )


def test_predict_with_empty_string_airline_code_is_treated_as_unseen(predictor, known_inputs):
    result = predictor.predict(
        airline_code="", origin=known_inputs["origin"], destination=known_inputs["destination"],
        dep_delay=8.0, air_time=90.0, distance=450.0,
    )
    assert result == 8.0  # unseen-label fallback, not a crash


def test_normalize_airport_code_leaves_non_k_prefixed_4letter_codes_unchanged():
    assert DelayPredictor._normalize_airport_code("PHNL") == "PHNL"


def test_normalize_airport_code_leaves_3letter_codes_unchanged():
    assert DelayPredictor._normalize_airport_code("JFK") == "JFK"


def test_normalize_airport_code_strips_leading_k_from_4letter_code():
    assert DelayPredictor._normalize_airport_code("KJFK") == "JFK"
