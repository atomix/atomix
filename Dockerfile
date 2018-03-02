# Dockerfile

FROM  java:8

MAINTAINER  Jordan Halterman <jordan.halterman@gmail.com>

RUN apt-get update && apt-get install -y iptables git stress

ARG YOURKIT_VERSION=2017.02-b75
ENV yourkit_version=$YOURKIT_VERSION
RUN wget https://www.yourkit.com/download/YourKit-JavaProfiler-$yourkit_version.zip \
  && unzip YourKit-JavaProfiler-$yourkit_version.zip -d /tmp \
  && rm YourKit-JavaProfiler-$yourkit_version.zip \
  && mkdir /atomix \
  && mv /tmp/YourKit-JavaProfiler-$(echo $yourkit_version | sed 's/\(.*\)-.*/\1/')/bin/linux-x86-64/libyjpagent.so /atomix/libyjpagent.so

ENV log_level=INFO
ENV console_log_level=INFO
ENV file_log_level=INFO

ENV profile false
RUN echo '#!/bin/bash' >> run_atomix \
  && echo 'if [ "$profile" = true ]; then \
    java -agentpath:/atomix/libyjpagent.so -jar -Datomix.logging.file.path=/data/logs -Datomix.logging.level=$log_level -Datomix.logging.console.level=$console_log_level -Datomix.logging.file.level=$file_log_level /atomix/atomix-agent.jar "$@"; \
    else \
    java -jar -Datomix.logging.file.path=/data/logs -Datomix.logging.level=$log_level -Datomix.logging.console.level=$console_log_level -Datomix.logging.file.level=$file_log_level /atomix/atomix-agent.jar "$@"; \
    fi' >> run_atomix \
  && chmod +x run_atomix

EXPOSE 5678
EXPOSE 5679

ADD /agent/target/atomix-agent.jar /atomix/atomix-agent.jar

ENTRYPOINT ["/bin/bash", "run_atomix"]
