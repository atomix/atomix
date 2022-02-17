# Dockerfile

FROM openjdk:8

ARG VERSION
RUN mkdir -p /opt/atomix
COPY target/atomix.tar.gz /opt/atomix/atomix.tar.gz
RUN tar -xvf /opt/atomix/atomix.tar.gz -C /opt/atomix && rm /opt/atomix/atomix.tar.gz

WORKDIR /opt/atomix

EXPOSE 5678
EXPOSE 5679

ENTRYPOINT ["./bin/atomix-agent"]
