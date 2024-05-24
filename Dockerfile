# Use the official Node.js 20 image.
# https://hub.docker.com/_/node
FROM node:20-slim

# Create and change to the app directory.
WORKDIR /usr/src/app

# Copy application dependency manifests to the container image.
# A wildcard is used to ensure both package.json AND package-lock.json are copied.
COPY package*.json ./

# Install production dependencies.
RUN npm install --omit=dev

# Copy local code to the container image.
COPY . ./

# set RUNTIME = prod environment var
ENV RUNTIME=prod
ENV PORT=8080

# Run the web service on container startup.
CMD [ "npm", "start" ]
