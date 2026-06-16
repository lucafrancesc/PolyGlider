# Gateway API (C# .NET)

Quick instructions to build and run the ingestion gateway locally.

## Prerequisites

- .NET 9 SDK
- Docker & Docker Compose (for local RabbitMQ)

## Build

From the repository root or `gateway-api-cs/` folder:

```bash
dotnet build gateway-api-cs.csproj
```

## Run (development)

1. Start the message broker:

```bash
docker-compose up -d
```

2. Run the gateway:

```bash
cd gateway-api-cs
dotnet run --project gateway-api-cs.csproj
```

By default the app listens on **http://localhost:5187** (see `Properties/launchSettings.json`).

## Environment Variables

Override broker connection settings via environment variables (double underscore = hierarchy separator):

- `RABBITMQ__HOST` — AMQP host (default: `localhost`)
- `RABBITMQ__PORT` — AMQP port (default: `5672`)
- `RABBITMQ__USER` — username (default: `guest`)
- `RABBITMQ__PASSWORD` — password (default: `guest`)

## Smoke test

Exercise end-to-end publishing with the example `POST /api/orders` curl from the [repository README](../README.md#example-request).

## Development notes

- Configuration: check `appsettings.Development.json` and `appsettings.json` for logging defaults.
- To publish a release build: `dotnet publish -c Release -o out`.
- Run unit tests: `cd gateway-api-cs-tests && dotnet test --filter "Category!=Integration"`
- Run integration tests (requires Docker): `dotnet test --filter "Category=Integration"`
