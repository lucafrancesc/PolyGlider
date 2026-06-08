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

By default the app listens on the port configured in `Properties/launchSettings.json` or `appsettings.json`. The sample curl in the repository README targets `http://localhost:5000/api/orders` — change the port if your local launch settings differ.

## Environment Variables

- `RABBITMQ__HOST` — AMQP host (default: `localhost`)
- `RABBITMQ__PORT` — AMQP port (default: `5672`)
- `RABBITMQ__USER` — username (default: `guest`)
- `RABBITMQ__PASSWORD` — password (default: `guest`)

Set these when running locally if your broker is non-default.

## Health & Smoke Test

After startup verify the API is reachable:

```bash
curl -v http://localhost:5000/health
```

Or use the example `POST /api/orders` curl from the repository README to exercise end-to-end publishing.

## Development notes

- Configuration: check `appsettings.Development.json` and `appsettings.json` for logging and connection defaults.
- To publish a release build: `dotnet publish -c Release -o out`.

If you want, I can also add a small launch script or sample `Dockerfile` for the gateway. Which would you prefer next?
