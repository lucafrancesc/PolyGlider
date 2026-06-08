ThisBuild / scalaVersion := "3.3.3" // Fixed: Capital 'T'
ThisBuild / version      := "0.1.0-SNAPSHOT" // Fixed: Capital 'T'

lazy val root = (project in file("."))
  .settings(
    name := "processing-engine-scala",
    libraryDependencies ++= Seq(
      // Functional effect framework and streaming support
      "org.typelevel" %% "cats-effect" % "3.5.4",
      "co.fs2"        %% "fs2-core"    % "3.9.4",
      
      // Functional streaming integration for RabbitMQ
      "dev.profunktor" %% "fs2-rabbit" % "5.1.0",
      
      // Pure functional JSON parsing (Circe)
      "io.circe" %% "circe-core"    % "0.14.6",
      "io.circe" %% "circe-generic" % "0.14.6",
      "io.circe" %% "circe-parser"  % "0.14.6"
    )
  )

// Test framework
libraryDependencies ++= Seq(
  "org.scalameta" %% "munit" % "0.7.29" % Test,
  "org.typelevel" %% "munit-cats-effect-3" % "1.0.7" % Test
)