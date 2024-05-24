#!/bin/bash

# Source environment variables from .env file
# shellcheck disable=SC1091
source .env 

# Check and set AWS environment variables
if [ -z "$AWS_ACCESS_KEY_ID" ]; then
  echo "AWS_ACCESS_KEY_ID is not set"
  exit 1
fi

if [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
  echo "AWS_SECRET_ACCESS_KEY is not set"
  exit 1
fi

if [ -z "$AWS_DEFAULT_REGION" ]; then
  export AWS_DEFAULT_REGION=us-east-1
  echo "AWS_DEFAULT_REGION is not set. Defaulting to us-east-1"
fi

# Check and set Mixpanel token
if [ -z "$MIXPANEL_TOKEN" ]; then
  echo "MIXPANEL_TOKEN is not set"
  exit 1
fi

# Ensure AWS credentials are set
export AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
export AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION

# Export other environment variables
export FRONTEND_URL=none
export NODE_ENV=prod
export REGION=US
export RUNTIME=AWS


# Ensure we are in the project root directory
cd "$(dirname "$0")/.." || exit

# Confirm the current directory has package.json
if [ ! -f package.json ]; then
  echo "Error: package.json not found in $(pwd). Ensure you are in the project root."
  exit 1
fi

# run the code through es-build
npm run build:lambda 

# Install the AWS SAM CLI if not already installed (uncomment if needed)
# brew tap aws/tap
# brew install aws-sam-cli

# Build the SAM application using the custom template file
sam build --template-file lambda.yml

sam deploy --guided --template-file lambda.yml \
  --stack-name mixpanel-proxy-stack \
  --capabilities CAPABILITY_IAM \
  --resolve-s3 \
  --parameter-overrides \
    MixpanelToken=${MIXPANEL_TOKEN} \
    FrontendUrl=${FRONTEND_URL} \
    Runtime=${RUNTIME} \
    Region=${REGION} \
    Platform=${PLATFORM}