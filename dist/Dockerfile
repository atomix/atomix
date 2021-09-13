# Dockerfile

FROM openjdk:8

RUN apt-get update -y && apt-get install -y iptables stress

ARG VERSION
RUN mkdir -p /opt/atomix
COPY target/atomix.tar.gz /opt/atomix/atomix.tar.gz
RUN tar -xvf /opt/atomix/atomix.tar.gz -C /opt/atomix && rm /opt/atomix/atomix.tar.gz

WORKDIR /opt/atomix

EXPOSE 5678
EXPOSE 5679

RUN groupadd -g 1000 atomix && useradd -r -s /bin/false -u 1000 -g atomix atomix && \
chown -R 1000:1000 /opt/atomix
USER atomix

ENTRYPOINT ["./bin/atomix-agent"]
