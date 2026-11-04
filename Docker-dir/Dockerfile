# Build stage: compile the sources and produce a single shaded jar.
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B package

# Runtime stage. Which class runs is chosen by the compose service's `command`.
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /build/target/DockerMultiContainerKafka.jar /app/app.jar
ENTRYPOINT ["java", "-cp", "/app/app.jar"]
CMD ["MyKafkaConsumer"]
