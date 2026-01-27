# Run only basedata
docker compose --profile basedata up
# Run basedata + metadata
docker compose --profile basedata --profile metadata up
# Run heat-related tools
docker compose --profile heat up
# Run everything
docker compose --profile all up
# Or use environment variable
COMPOSE_PROFILES=basedata,metadata docker compose up


docker compose up -d --no-deps <service_name>