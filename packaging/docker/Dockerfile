FROM eclipse-temurin:11-jre

ENV IGNITE_HOME /opt/ignite

WORKDIR $IGNITE_HOME
COPY dist .
EXPOSE 3344 10300
ENTRYPOINT ["bin/docker-entrypoint.sh"]
