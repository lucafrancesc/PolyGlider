# The generic "_1.x" tag (any sbt 1.x) is used instead of a fully pinned version because the
# upstream registry periodically prunes older fully-pinned tags (e.g. the previous
# 21.0.5_11_1.10.5_3.3.3 pin 404'd); sbt resolves the project's actual Scala 3.3.3 compiler at
# build time regardless of what's pre-cached in this base image.
FROM sbtscala/scala-sbt:eclipse-temurin-21.0.11_10_1.x AS build
WORKDIR /src

COPY project/ project/
COPY build.sbt .
RUN sbt update

COPY src/ src/
RUN sbt stage

FROM eclipse-temurin:21-jre-alpine AS runtime
# sbt-native-packager's generated start script is a bash script; Alpine's base image only
# ships /bin/sh.
RUN apk add --no-cache bash
WORKDIR /app
COPY --from=build /src/target/universal/stage/ .
ENTRYPOINT ["bin/processing-engine-scala"]
