# Dockerfile

FROM  java:8

MAINTAINER  Jordan Halterman <jordan.halterman@gmail.com>

EXPOSE 5678
EXPOSE 5679

ADD /agent/target/atomix-agent.jar atomix-agent.jar

ENTRYPOINT ["java", "-jar", "atomix-agent.jar"]
