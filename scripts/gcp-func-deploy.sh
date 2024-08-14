#!/bin/bash

# shellcheck disable=SC1091
source .env

gcloud config set functions/region us-central1

# Deploy the function
gcloud functions deploy mixpanel_proxy \
  --runtime nodejs20 \
  --trigger-http \
  --allow-unauthenticated \
  --entry-point start \
  --source . \
  --set-env-vars FRONTEND_URL=none,NODE_ENV=prod,REGION=US,RUNTIME='cloud_functions'

