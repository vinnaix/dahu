# Use the official maven/Java 8 image to create a build artifact.
# https://hub.docker.com/_/maven
FROM maven:3.5-jdk-8-alpine as builder

# Copy local code to the container image.
WORKDIR /app

COPY run/lib /app/lib

# build image for AODocs AtlanticLabs
#COPY run/configAtlanticLabs /app/config
#COPY run/dahu-search-connector.json /app/dahu-search-connector.json

# build image for Customer config
COPY run/configStaging /app/config
COPY run/co-opc-service.json /app/co-opc-service.json

RUN mkdir /var/deflogs

# PORT variable is taken from run-time if set, otherwise it defaults to this  value
ENV PORT=10102

EXPOSE $PORT


HEALTHCHECK --interval=120s --timeout=5s --retries=2 CMD curl --silent --fail localhost:$PORT/api/push/vector_queue || exit 1

# Run the web service on container startup.

ENTRYPOINT ["java","-Djava.library.path=./lib/native/mac-x86_64","-jar","./lib/DahuDEFServer.jar","-cdir","/app/config","-c","dahuConfig.json","-console","-cloud"]
