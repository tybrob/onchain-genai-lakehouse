FROM maven:3.9.6-eclipse-temurin-11 AS builder

WORKDIR /app

COPY pom.xml .

RUN mvn dependency:go-offline

COPY src ./src

RUN mvn clean package -DskipTests

FROM apache/flink:1.20.0-java11

WORKDIR /opt/flink/usrlib

COPY --from=builder /app/target/flink-btc-enrichment-1.0-SNAPSHOT.jar /opt/flink/usrlib/flink-btc-enrichment.jar
