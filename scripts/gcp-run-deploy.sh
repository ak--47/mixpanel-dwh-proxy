#!/bin/bash

# shellcheck disable=SC1091
source .env

gcloud config set run/region us-central1
# docker build -t express-proxy .
docker buildx build --platform linux/amd64 -t express-proxy .
docker tag express-proxy gcr.io/mixpanel-gtm-training/express-proxy
docker push gcr.io/mixpanel-gtm-training/express-proxy
gcloud run deploy express-proxy \
  --image gcr.io/mixpanel-gtm-training/express-proxy \
  --platform managed \
  --project mixpanel-gtm-training \
  --allow-unauthenticated \
  --set-env-vars FRONTEND_URL=none,NODE_ENV=prod,REGION=US,RUNTIME='cloud_run'

