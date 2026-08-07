"""
Retrains the arrival-delay regression model used by ml/predictor.py.

Usage (from anywhere, run from the repo or not — paths are resolved
relative to this file, not the current working directory):

    python ml/train.py

Requires T_ONTIME_MARKETING.csv in the repo root (gitignored — obtain your
own copy of the BTS On-Time Performance dataset with that name) and writes
ml/model.pkl, which ml/predictor.py loads at server startup. ml/model.pkl is
also gitignored, so this must be re-run after every fresh clone before the
backend can start.
"""
from pathlib import Path

import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.preprocessing import LabelEncoder
import pickle

REPO_ROOT = Path(__file__).resolve().parent.parent
CSV_PATH = REPO_ROOT / "T_ONTIME_MARKETING.csv"
MODEL_PATH = REPO_ROOT / "ml" / "model.pkl"

# ── 1. Load ───────────────────────────────────────────────────────────────────
df = pd.read_csv(CSV_PATH)

# ── 2. Clean ──────────────────────────────────────────────────────────────────
# only keep flights that actually departed and were delayed
df = df[df["CANCELLED"] != 1.0]
df = df[df["DEP_DELAY"] > 0]
df = df.dropna(subset=["DEP_DELAY", "ARR_DELAY", "AIR_TIME", "DISTANCE"])

# ── 3. Features ───────────────────────────────────────────────────────────────
# encode categorical columns
le_carrier = LabelEncoder()
le_origin  = LabelEncoder()
le_dest    = LabelEncoder()

df["carrier_enc"] = le_carrier.fit_transform(df["OP_UNIQUE_CARRIER"])
df["origin_enc"]  = le_origin.fit_transform(df["ORIGIN"])
df["dest_enc"]    = le_dest.fit_transform(df["DEST"])

# fill delay breakdown columns with 0 if missing
for col in ["CARRIER_DELAY","WEATHER_DELAY","NAS_DELAY","SECURITY_DELAY","LATE_AIRCRAFT_DELAY"]:
    df[col] = df[col].fillna(0)

features = [
    "carrier_enc",
    "origin_enc",
    "dest_enc",
    "DEP_DELAY",
    "AIR_TIME",
    "DISTANCE",
    "CARRIER_DELAY",
    "WEATHER_DELAY",
    "NAS_DELAY",
    "LATE_AIRCRAFT_DELAY",
]

X = df[features]
y = df["ARR_DELAY"]  # predicting how bad the arrival delay will be

# ── 4. Train ──────────────────────────────────────────────────────────────────
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

model = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
model.fit(X_train, y_train)

# ── 5. Evaluate ───────────────────────────────────────────────────────────────
preds = model.predict(X_test)
mae   = mean_absolute_error(y_test, preds)
r2    = r2_score(y_test, preds)
print(f"MAE:  {mae:.2f} minutes")
print(f"R²:   {r2:.4f}")

# ── 6. Save ───────────────────────────────────────────────────────────────────
with open(MODEL_PATH, "wb") as f:
    pickle.dump({
        "model": model,
        "le_carrier": le_carrier,
        "le_origin": le_origin,
        "le_dest": le_dest,
        "features": features,
    }, f)

print(f"Model saved to {MODEL_PATH}")