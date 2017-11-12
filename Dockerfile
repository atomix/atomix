# Dockerfile

FROM  java:8

MAINTAINER  Jordan Halterman <jordan.halterman@gmail.com>

EXPOSE 5678
EXPOSE 5679

ADD /server/target/atomix-server.jar atomix-server.jar

ENTRYPOINT ["java", "-jar", "atomix-server.jar"]
