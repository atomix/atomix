# Dockerfile

FROM java:8

ARG VERSION
RUN mkdir -p /opt/atomix
COPY target/atomix-agent-${VERSION}.tar.gz /opt/atomix/atomix-agent-${VERSION}.tar.gz
RUN tar -xvf /opt/atomix/atomix-agent-${VERSION}.tar.gz -C /opt/atomix && rm /opt/atomix/atomix-agent-${VERSION}.tar.gz

WORKDIR /opt/atomix

EXPOSE 5678
EXPOSE 5679

ENTRYPOINT ["./bin/atomix-agent"]
