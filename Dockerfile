# ----- SCHEMA BUILDER -----
FROM maven:3.6.0-jdk-8-alpine AS schema-builder
WORKDIR /tmp

# inject the avro schema sources
COPY data/schema/avro /tmp

# run avro tools to compile the schema files
RUN java -jar avro-tools-1.8.2.jar compile schema . generated/


# ----- FLINK JOB COMPILATION -----
FROM maven:3.6.0-jdk-8-alpine AS job-builder
WORKDIR /tmp

# allow parametrization of the consumer to be built
ARG CONSUMER_ROOT="social-network-analysis"

# inject the maven dependencies and install them
COPY ${CONSUMER_ROOT}/pom.xml /tmp/pom.xml
RUN mvn install

# inject the flink job sources
COPY ${CONSUMER_ROOT}/src /tmp/src

# inject the generated avro schema
COPY --from=schema-builder /tmp/generated /tmp/target/generated-sources/avro

# package the flink job into a jar
RUN mvn package
RUN ls /tmp/target

# ----- FLINK JOB CLUSTER -----
# https://github.com/apache/flink/blob/release-1.7/flink-container/docker/Dockerfile
FROM java:8-jre-alpine

# Install requirements
RUN apk add --no-cache bash snappy

# allow parametrization of the jar to use
ARG JAR_NAME="social-network-analysis-1.0.0.jar"
ARG FLINK_VERSION="1.7.2"
ARG SCALA_VERSION="2.11"

# Flink environment variables
ENV FLINK_FILENAME=flink-${FLINK_VERSION}-bin-scala_${SCALA_VERSION}.tgz
ENV FLINK_INSTALL_PATH=/opt
ENV FLINK_HOME $FLINK_INSTALL_PATH/flink
ENV FLINK_LIB_DIR $FLINK_HOME/lib
ENV PATH $PATH:$FLINK_HOME/bin

# install flink
ADD ${FLINK_FILENAME} ${FLINK_INSTALL_PATH}

# inject the compiled job jar
COPY --from=job-builder /tmp/target/${JAR_NAME} $FLINK_INSTALL_PATH/job.jar
COPY docker-entrypoint.sh /

RUN set -x && \
  ln -s $FLINK_INSTALL_PATH/flink-* $FLINK_HOME && \
  ln -s $FLINK_INSTALL_PATH/job.jar $FLINK_LIB_DIR && \
  addgroup -S flink && adduser -D -S -H -G flink -h $FLINK_HOME flink && \
  chown -R flink:flink $FLINK_INSTALL_PATH/flink-* && \
  chown -h flink:flink $FLINK_HOME && \
  chmod +x /docker-entrypoint.sh

USER flink
EXPOSE 8081 6123
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["job-cluster", "-j", "ch.ethz.infk.dspa.App"]
