# Production image for the React/D3 frontend.
#
# CRA bakes REACT_APP_* vars into the JS bundle at *build* time, not
# runtime (frontend/src/hooks/useFlightData.js reads
# process.env.REACT_APP_BACKEND_URL once, at import). The build ARG below
# defaults to unset, which the app itself then defaults to
# "http://localhost:8000" — correct for docker-compose, where the backend
# container publishes 8000 to the host and the browser (running on the
# host, not in a container) reaches it there. Override at build time
# (--build-arg REACT_APP_BACKEND_URL=https://api.example.com) for any
# deploy where the backend isn't at localhost:8000, e.g. AWS.

FROM node:20-alpine AS build
WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci

COPY . .
ARG REACT_APP_BACKEND_URL
ENV REACT_APP_BACKEND_URL=${REACT_APP_BACKEND_URL}
RUN npm run build

FROM nginx:1.27-alpine
COPY --from=build /app/build /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80
