FROM anapsix/alpine-java:8_server-jre

ENV HOME=/opt/atomix
ENV ATOMIX_DIR=/var/lib/atomix
ENV DATA_DIR=${ATOMIX_DIR}/data

ARG VERSION

RUN addgroup -S atomix && adduser -S atomix -G atomix && \
mkdir -p ${HOME} && mkdir -p ${DATA_DIR}

COPY target/atomix.tar.gz /opt/atomix/atomix.tar.gz
RUN tar -xvf /opt/atomix/atomix.tar.gz -C /opt/atomix && rm /opt/atomix/atomix.tar.gz && \
 chown -R atomix:atomix ${ATOMIX_DIR} && chown -R atomix:atomix /opt/atomix

WORKDIR /opt/atomix

EXPOSE 5678
EXPOSE 5679

USER atomix

ENTRYPOINT ["./bin/atomix-agent"]
