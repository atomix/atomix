# Dockerfile

FROM anapsix/alpine-java:8_server-jre

ARG VERSION
RUN mkdir -p /root/atomix
COPY target/atomix-agent-${VERSION}.tar.gz /root/atomix/atomix-agent-${VERSION}.tar.gz
RUN tar -xvf /root/atomix/atomix-agent-${VERSION}.tar.gz -C /root/atomix && rm /root/atomix/atomix-agent-${VERSION}.tar.gz

WORKDIR /root/atomix

EXPOSE 5678
EXPOSE 5679

ENTRYPOINT ["./bin/atomix-agent"]
