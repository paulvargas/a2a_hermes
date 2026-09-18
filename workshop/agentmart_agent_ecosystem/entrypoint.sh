#!/bin/sh
# Seed the AgentMart SQLite DB on first run (or whenever the container's
# data volume/layer doesn't already have it), then exec the given command.
set -e

if [ ! -f "data/agentmart.db" ]; then
    echo "entrypoint: data/agentmart.db not found, seeding..."
    python seed_data.py
fi

exec "$@"
