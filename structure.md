polyglider/                  <-- Your Git Repository Root
├── docker-compose.yml       # Shared infrastructure (RabbitMQ / Databases)
├── README.md                # System Architecture Documentation
│
├── gateway-api-cs/          # C# .NET Ingestion Service
│   ├── Program.cs
│   └── gateway-api-cs.csproj
│
├── processing-engine-scala/ # Scala 3 Processing Service
│   ├── build.sbt
│   └── src/main/scala/
│
├── analytics-worker-py/     # Python Analytics Service
│   ├── main.py
│   └── requirements.txt
│
└── tools/                   # Development & testing tools
    └── load-tester/        # Locust load tester (locustfile.py, README, requirements)
