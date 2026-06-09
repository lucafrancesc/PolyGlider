# Processing Engine (Scala 3)

This module contains the core deterministic processing engine implemented in Scala 3 using Cats Effect and FS2.

## Prerequisites

- Java 17+ (or the JDK version targeted by your `build.sbt`)
- sbt (recommended) or a compatible build tool
- Docker & Docker Compose (for local RabbitMQ)

## Build

From the `processing-engine-scala` directory:

```bash
sbt compile
```

## Run (development)

1. Start the message broker if not already running:

```bash
docker-compose up -d
```

2. Run the processing engine via sbt:

```bash
cd processing-engine-scala
sbt run
```

By default the app will read broker connection settings from environment variables or the application config (implementations vary). If the project uses `application.conf`, set `RABBITMQ_HOST` and `RABBITMQ_PORT` accordingly.

## Testing

Run unit tests with:

```bash
sbt test
```

Notes:
- Unit tests use an in-memory H2 datasource for fast execution.
- Integration tests use Testcontainers (Docker required). Run the full test-suite with `sbt test` and ensure Docker is available.

## Development notes

- Project layout: place source under `src/main/scala` and tests under `src/test/scala`.
- Use Cats Effect `IOApp` as the application entry point to make local execution and testing straightforward.
- Use `fs2.Stream` with a bounded queue or back-pressure-aware consumer to process events from RabbitMQ.
- Consider adding `docker-compose.override.yml` for developer convenience to set service names or ports.

## Local DB migrations

The processor uses Flyway migrations bundled in `src/main/resources/db/migration/`.

Start local infrastructure (RabbitMQ + Postgres):

```bash
# from repository root
docker-compose up -d
```

Run the processing engine (it runs Flyway on startup):

```bash
cd processing-engine-scala
sbt run
```

If you prefer to run migrations manually before starting the app, set the DB env vars and run the app which will execute Flyway on startup as well:

```bash
export PG_URL=jdbc:postgresql://localhost:5432/polyglider
export PG_USER=postgres
export PG_PASSWORD=postgres
sbt run
```

If you want, I can scaffold a minimal `build.sbt`, an `IOApp` starter, and an `application.conf` example. Should I create those starter files?
