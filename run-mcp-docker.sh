#!/bin/bash

docker run --rm -i --network host \
  -e DATABASE_URL="postgres://postgres:postgres@localhost:5432/testdb?sslmode=disable" \
  postgres-mcp
