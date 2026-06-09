ThisBuild / scalaVersion := "3.3.3"
ThisBuild / version := "0.1.0-SNAPSHOT"

lazy val root = (project in file("."))
  .settings(
    name := "processing-engine-scala",
    libraryDependencies ++= Seq(
      // Core libraries
      "org.typelevel" %% "cats-effect" % "3.7.0",
      "co.fs2" %% "fs2-core" % "3.13.0",

      // RabbitMQ integration
      "dev.profunktor" %% "fs2-rabbit" % "5.5.2",
      "dev.profunktor" %% "fs2-rabbit-circe" % "5.5.2",

      // JSON (Circe)
      "io.circe" %% "circe-core" % "0.14.15",
      "io.circe" %% "circe-generic" % "0.14.15",
      "io.circe" %% "circe-parser" % "0.14.15",

      // Logging
      "ch.qos.logback" % "logback-classic" % "1.5.34",
      "org.typelevel" %% "log4cats-slf4j" % "2.8.0",

      // Database dependencies for Postgres ledger
      "org.postgresql" % "postgresql" % "42.7.7",
      "org.tpolecat" %% "doobie-core" % "1.0.0-RC12",
      "org.tpolecat" %% "doobie-hikari" % "1.0.0-RC12",
      "org.tpolecat" %% "doobie-postgres" % "1.0.0-RC12",

      // Flyway for DB migrations
      "org.flywaydb" % "flyway-core" % "12.8.1",

      // Typesafe config
      "com.typesafe" % "config" % "1.4.2",

      // Testing
      "org.scalameta" %% "munit" % "1.3.3" % Test,
      "org.typelevel" %% "munit-cats-effect-3" % "1.0.7" % Test
    )
  )
