FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src
COPY gateway-api-cs.csproj .
RUN dotnet restore gateway-api-cs.csproj
COPY . .
RUN dotnet publish gateway-api-cs.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS runtime
WORKDIR /app
COPY --from=build /app .
ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080
ENTRYPOINT ["dotnet", "gateway-api-cs.dll"]
