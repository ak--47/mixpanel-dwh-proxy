#!/bin/bash



# Check if Azure Functions Core Tools are installed
if ! command -v func &> /dev/null
then
    echo "Azure Functions Core Tools are not installed. Installing..."
	brew tap azure/functions
	brew install azure-functions-core-tools@4
	# if upgrading on a machine that has 2.x or 3.x installed:
	brew link --overwrite azure-functions-core-tools@4
    # npm install -g azure-functions-core-tools@3 --unsafe-perm true
else
    echo "Azure Functions Core Tools are already installed."
fi

# Export environment variables
export FRONTEND_URL=none
export NODE_ENV=prod
export RUNTIME=AZURE
export REGION=US

# Deploy the function
func azure functionapp publish mp-dwh-proxy --publish-local-settings -y \
  --set MIXPANEL_TOKEN=none \
  --set FRONTEND_URL=none \
  --set RUNTIME=prod \
  --set REGION=US \
  --set PLATFORM=AZURE
