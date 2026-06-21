polyglider/                  <-- Git repository root
├── docker-compose.yml       # Shared infrastructure (RabbitMQ + Postgres)
├── README.md                # System architecture and getting started
├── structure.md             # This file
│
├── gateway-api-cs/          # C# .NET ingestion service (implemented)
│   ├── Program.cs
│   ├── gateway-api-cs.csproj
│   └── README.md
│
├── processing-engine-scala/ # Scala 3 processing service (implemented)
│   ├── build.sbt
│   ├── src/main/scala/
│   ├── src/main/resources/  # application.conf, Flyway migrations
│   └── README.md
│
├── analytics-worker-python/ # Python analytics service (implemented)
│   ├── main.py
│   ├── requirements.txt
│   └── Dockerfile
│
├── run-all.sh               # Start all services (--analytics flag for Python worker)
│
└── tools/
    └── load-tester/         # Locust load tester (implemented)
        ├── locustfile.py
        ├── requirements.txt
        └── README.md
