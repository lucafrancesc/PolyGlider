# Processing Engine (Scala 3)

This module contains the core deterministic processing engine implemented in Scala 3 using Cats Effect, Doobie, and Flyway.

## Prerequisites

- Java 17+ (or the JDK version targeted by your `build.sbt`)
- sbt (recommended) or a compatible build tool
- Docker & Docker Compose (for local RabbitMQ and Postgres)

## Build

From the `processing-engine-scala` directory:

```bash
sbt compile
```

## Run (development)

1. Start local infrastructure if not already running:

```bash
docker-compose up -d
```

2. Configure Postgres for ledger persistence (optional but recommended):

The bundled `application.conf` defaults to `jdbc:postgresql://localhost:5432/polyglider` with `app.db.runMigrations = false`. Docker Compose creates database **`polyglider_inventory`**. To persist ledger data locally, either:

- Create a `polyglider` database in Postgres and set `app.db.runMigrations = true` in `application.conf`, or
- Change `app.db.url` to `jdbc:postgresql://localhost:5432/polyglider_inventory` and set `runMigrations = true`.

3. Run the processing engine:

```bash
cd processing-engine-scala
sbt run
```

## Broker configuration

RabbitMQ connection settings are read from **environment variables** in `RabbitConsumer` (not from `application.conf`):

- `RABBIT_HOST` (default: `127.0.0.1`)
- `RABBIT_PORT` (default: `5672`)
- `RABBIT_USER` (default: `guest`)
- `RABBIT_PASS` (default: `guest`)

## Testing

Run unit tests with:

```bash
sbt test
```

Notes:

- Unit tests use an in-memory H2 datasource for fast execution.
- Testcontainers integration tests are **planned** (dependencies are declared in `build.sbt`, but no integration tests exist yet).

## Development notes

- Project layout: source under `src/main/scala`, tests under `src/test/scala`.
- Entry point: Cats Effect `IOApp` in `Main.scala`.
- Consumer: Java RabbitMQ client with a Cats Effect bounded queue and worker fibers (FS2 is on the classpath but not used by the consumer yet).
- Flyway migrations: `src/main/resources/db/migration/`.

## Local DB migrations

Flyway migrations run at startup when `app.db.runMigrations = true` in `application.conf`.

```bash
# from repository root
docker-compose up -d

cd processing-engine-scala
sbt run
```
