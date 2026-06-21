ThisBuild / scalaVersion := "3.3.3"
ThisBuild / version := "0.1.0-SNAPSHOT"

lazy val root = (project in file("."))
  .enablePlugins(JavaAppPackaging)
  .settings(
    name := "processing-engine-scala",
    Compile / run / fork := true,
    libraryDependencies ++= Seq(
      // Core libraries
      "org.typelevel" %% "cats-effect" % "3.7.0",
      "co.fs2" %% "fs2-core" % "3.13.0",

      // RabbitMQ Java client (used directly by RabbitConsumer)
      "com.rabbitmq" % "amqp-client" % "5.21.0",

      // JSON (Circe)
      "io.circe" %% "circe-core" % "0.14.15",
      "io.circe" %% "circe-generic" % "0.14.15",
      "io.circe" %% "circe-parser" % "0.14.15",

      // Logging
      "ch.qos.logback" % "logback-classic" % "1.5.34",
      "org.typelevel" %% "log4cats-slf4j" % "2.8.0",

      // Database dependencies for Postgres ledger
      "org.postgresql" % "postgresql" % "42.7.7",
      "com.h2database" % "h2" % "2.2.220" % Test,
      "org.tpolecat" %% "doobie-core" % "1.0.0-RC12",
      "org.tpolecat" %% "doobie-hikari" % "1.0.0-RC12",
      "org.tpolecat" %% "doobie-postgres" % "1.0.0-RC12",

      // Flyway for DB migrations (flyway-database-postgresql required separately since Flyway 10)
      "org.flywaydb" % "flyway-core" % "12.8.1",
      "org.flywaydb" % "flyway-database-postgresql" % "12.8.1",

      // Typesafe config
      "com.typesafe" % "config" % "1.4.2",

      // Prometheus metrics + /metrics HTTP endpoint
      "io.prometheus" % "simpleclient" % "0.16.0",
      "io.prometheus" % "simpleclient_httpserver" % "0.16.0",

      // OpenTelemetry tracing — autoconfigure reads OTEL_EXPORTER_OTLP_ENDPOINT /
      // OTEL_SERVICE_NAME env vars and wires up the OTLP exporter via ServiceLoader
      "io.opentelemetry" % "opentelemetry-api" % "1.43.0",
      "io.opentelemetry" % "opentelemetry-sdk" % "1.43.0",
      "io.opentelemetry" % "opentelemetry-sdk-extension-autoconfigure" % "1.43.0",
      "io.opentelemetry" % "opentelemetry-exporter-otlp" % "1.43.0",

      // Testing
      "io.opentelemetry" % "opentelemetry-sdk-testing" % "1.43.0" % Test,
      "org.scalameta" %% "munit" % "1.3.3" % Test,
      "org.typelevel" %% "munit-cats-effect-3" % "1.0.7" % Test,
      "org.testcontainers" % "testcontainers" % "1.19.0" % Test,
      "com.dimafeng" %% "testcontainers-scala-scalatest" % "0.40.11" % Test,
      "com.dimafeng" %% "testcontainers-scala-postgresql" % "0.40.11" % Test,
      "com.dimafeng" %% "testcontainers-scala-rabbitmq" % "0.40.11" % Test,

      // Contract testing — Pact JVM consumer DSL (Java artifact, single %)
      "au.com.dius.pact.consumer" % "junit5" % "4.6.14" % Test
    )
  )
