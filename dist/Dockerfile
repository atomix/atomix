# Dockerfile

FROM openjdk:8

RUN apt-get update -y && apt-get install -y iptables stress

ENV HOME=/opt/atomix
ENV ATOMIX_DIR=/var/lib/atomix
ENV DATA_DIR=${ATOMIX_DIR}/data

ARG VERSION

RUN groupadd -g 2000 atomix && \
useradd -r -s /bin/false -u 1000 -g atomix atomix && \
mkdir -p ${HOME} && \
mkdir -p ${DATA_DIR} && \
chown -R 1000:2000 ${ATOMIX_DIR} && \
chown -R 1000:2000 ${HOME}

COPY target/atomix.tar.gz ${HOME}/atomix.tar.gz
RUN tar -xvf ${HOME}/atomix.tar.gz -C ${HOME} && rm ${HOME}/atomix.tar.gz

WORKDIR ${HOME}

EXPOSE 5678
EXPOSE 5679

ENTRYPOINT ["./bin/atomix-agent"]
