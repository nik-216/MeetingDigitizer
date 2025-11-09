FROM wurstmeister/kafka:latest

USER root

RUN apt-get update && \
    apt-get install -y netcat-traditional && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY --chmod=755 kafka-setup.sh /kafka-setup.sh

# The base image doesn’t define a "kafka" user — use root
USER root
