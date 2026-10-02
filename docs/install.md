# Installation

## Prerequisites

- Docker + Docker Compose v2
- An external Docker network named `dockge_default` (create with `docker network create dockge_default` if not present)
- Optional: a llama.cpp server for slot management (default endpoint `http://localhost:9001`)
- Optional: a compaction model endpoint (default `http://localhost:11434/api/chat`)

## Step-by-step

### 1. Clone and configure

```bash
git clone https://github.com/StefRush2099/Zorg_MemoryDB_MCP.git
cd Zorg_MemoryDB_MCP
cp .env.example .env
```

Edit `.env`:
```env
POSTGRES_PASSWORD=YourStrongPasswordHere
POSTGRES_USER=ollama_cpp_mcp
POSTGRES_NATIVE_TOKEN=a64-hex-native-token
```

Generate a native token:
```bash
openssl rand -hex 32
```

### 2. Build the plugin image

The `memorydb-plugin` uses a locally-built image. Build it from the repo root:

```bash
# The build context is the repo root; Dockerfile is in memorydb-plugin-build/
docker build -t local/zorg-memorydb-plugin:1 ./memorydb-plugin-build
```

### 3. Create the external network

```bash
docker network create dockge_default
```

### 4. Start the stack

```bash
docker compose up -d
```

Wait for the DB to be ready (check `docker logs db` for "ready to accept connections"), then:

```bash
docker compose up -d --force-recreate postgres-mcp memorydb-openapi memorydb-plugin
```

### 5. Initialize the database

Connect via psql or adminer (http://localhost:8080) and run the schema:

```sql
\i db/schema.sql
```

### 6. Verify

```bash
# postgres-mcp SSE endpoint
curl -s http://localhost:7779/sse | head -5

# memorydb-openapi
curl -s http://localhost:1781/health

# memorydb-plugin
curl -s http://localhost:1782/health
```

## Updating

```bash
git pull
docker compose up -d
```

For schema migrations, apply incremental DDL manually (the schema is versioned via `zorg_memory` rows, not auto-migration).
