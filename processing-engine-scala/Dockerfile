FROM sbtscala/scala-sbt:eclipse-temurin-21.0.5_11_1.10.5_3.3.3 AS build
WORKDIR /src

COPY project/ project/
COPY build.sbt .
RUN sbt update

COPY src/ src/
RUN sbt stage

FROM eclipse-temurin:21-jre-alpine AS runtime
WORKDIR /app
COPY --from=build /src/target/universal/stage/ .
ENTRYPOINT ["bin/processing-engine-scala"]
