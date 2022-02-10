FROM maven:3.6-amazoncorretto-8
FROM 602401143452.dkr.ecr.us-east-1.amazonaws.com/spark/emr-6.3.0:latest
USER root
WORKDIR /tmp/
ADD pom.xml /tmp
COPY --from=0 /usr/share/maven /usr/share/maven
RUN /usr/share/maven/bin/mvn dependency:tree
RUN /usr/share/maven/bin/mvn dependency:copy-dependencies -DoutputDirectory=/usr/lib/spark/jars/
RUN mkdir /mnt/s3 \
    && chown spark:spark /mnt/s3
USER spark:spark
ENV SPARK_NO_DAEMONIZE=true
ENTRYPOINT [ "/usr/lib/spark/sbin/start-history-server.sh" ]
