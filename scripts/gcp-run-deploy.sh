#!/bin/bash

# shellcheck disable=SC1091
source .env

gcloud config set run/region us-central1
# docker build -t mixpanel-proxy .
docker buildx build --platform linux/amd64 -t mixpanel-proxy .
docker tag mixpanel-proxy gcr.io/mixpanel-gtm-training/mixpanel-proxy
docker push gcr.io/mixpanel-gtm-training/mixpanel-proxy
gcloud run deploy mixpanel-proxy \
  --image gcr.io/mixpanel-gtm-training/mixpanel-proxy \
  --platform managed \
  --project mixpanel-gtm-training \
  --allow-unauthenticated \
  --set-env-vars FRONTEND_URL=none,NODE_ENV=prod,REGION=US,RUNTIME='cloud_run'

