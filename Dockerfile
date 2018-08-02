# Dockerfile

FROM  java:8

MAINTAINER  Jordan Halterman <jordan.halterman@gmail.com>

RUN apt-get update && apt-get install -y iptables git stress

RUN mkdir /atomix

ARG VERSION
COPY /dist/target/atomix-agent-${VERSION}.tar.gz /atomix/atomix-agent-${VERSION}.tar.gz
RUN tar -xvf /atomix/atomix-agent-${VERSION}.tar.gz -C /atomix && rm /atomix/atomix-agent-${VERSION}.tar.gz

EXPOSE 5678
EXPOSE 5679

ENTRYPOINT ["/atomix/bin/atomix-agent"]
