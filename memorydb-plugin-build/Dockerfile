FROM crystaldba/postgres-mcp:latest

# Override the server.py and add the plugin
COPY server.py /app/src/postgres_mcp/server.py
COPY memorydb-plugin.py /app/memorydb-plugin.py

WORKDIR /app
