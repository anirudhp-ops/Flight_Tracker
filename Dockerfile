# Production image for the FastAPI/uvicorn backend (flight_tracker/).
#
# ml/model.pkl is deliberately NOT copied in here: it's gitignored, ~955MB
# (bigger than every dependency in this image combined), and produced by
# ml/train.py from T_ONTIME_MARKETING.csv, which is also gitignored and not
# part of this build context. Baking either into the image would make every
# build slow and the image enormous for no benefit. Instead the model is
# mounted at runtime — see docker-compose.yml's backend service
# (`./ml/model.pkl:/app/ml/model.pkl:ro`). Train it on the host first with
# `python ml/train.py` if ml/model.pkl doesn't exist yet.
#
# Multi-stage so the final image ships a venv, not pip's build cache.

FROM python:3.13-slim AS builder
WORKDIR /app

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

FROM python:3.13-slim
WORKDIR /app

# Runs as an unprivileged user — nothing here needs root.
RUN groupadd -r flighttracker && useradd -r -g flighttracker flighttracker

COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY flight_tracker/ ./flight_tracker/
# predictor.py + train.py only; model.pkl is mounted, not copied (see above).
COPY ml/predictor.py ml/train.py ./ml/
COPY ml/fixtures/ ./ml/fixtures/

RUN chown -R flighttracker:flighttracker /app
USER flighttracker

EXPOSE 8000

# No --reload: this is the production entrypoint. Local hot-reload dev
# still runs on the host per README.md (`uvicorn ... --reload`).
CMD ["uvicorn", "flight_tracker.server:app", "--host", "0.0.0.0", "--port", "8000"]
