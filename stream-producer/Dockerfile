FROM maven:3.6.0-jdk-8-alpine AS builder
WORKDIR /tmp

# allow parametrization of the producer to be built
ARG PRODUCER_ROOT="stream-producer"

# inject the wait-for entrypoint
COPY ${PRODUCER_ROOT}/wait-for.sh /tmp/wait-for.sh

# inject the maven dependencies and install them
COPY ${PRODUCER_ROOT}/pom.xml /tmp/pom.xml
RUN mvn install

# inject the producer sources
COPY ${PRODUCER_ROOT}/src /tmp/src

# package the producer into a jar
RUN mvn package -DskipTests=true


FROM openjdk:8-jre-alpine

COPY --from=builder /tmp/wait-for.sh /wait-for.sh
COPY --from=builder /tmp/target/stream-producer-1.0.0.jar /opt/producer.jar

RUN chmod +x /wait-for.sh

ENTRYPOINT ["/wait-for.sh", "all", "java", "-jar", "/opt/producer.jar", "-worker", "1"]
