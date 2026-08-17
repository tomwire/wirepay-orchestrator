#!/usr/bin/env bash
# One-shot setup for wirepay-orchestrator local dev.
# Run:  ./INIT.sh
# Thomas Wire <tom@thomaswire.com>
set -euo pipefail

cd /home/twire/repos/wirepay-orchestrator

echo "=== 1. Generating go.sum ==="
go mod tidy

echo "=== 2. Building Docker images ==="
docker compose up -d --build postgres redis zookeeper kafka

echo ""
echo "=== 3. Waiting for infrastructure ==="
sleep 8

echo "=== 4. Starting WPO services ==="
docker compose up -d wire-order wire-inventory wire-payment wire-notification wire-ledger

echo ""
echo "=== 5. Git init with GPG-signed initial commit ==="
git init -b main
git config user.name "Thomas Wire"
git config user.email "tom@thomaswire.com"
git config commit.gpgsign true

git add -A
GIT_AUTHOR_NAME="Thomas Wire" \
GIT_AUTHOR_EMAIL="tom@thomaswire.com" \
GIT_COMMITTER_NAME="Thomas Wire" \
GIT_COMMITTER_EMAIL="tom@thomaswire.com" \
git commit -S --gpg-sign="tom@thomaswire.com" \
  -m "feat: initial WPO scaffold — 5 microservices, docker-compose dev stack

- wire-order: checkout saga orchestrator (state-machine, idempotent)
- wire-inventory: stock reservation with Redlock + SELECT FOR UPDATE
- wire-payment: Stripe mock/real provider, idempotency tokens
- wire-notification: templated notifications with dead-letter queue
- wire-ledger: double-entry bookkeeping with auto balance updates

Infrastructure: Dockerfile (multi-stage), docker-compose.yml (PG, Redis, Kafka),
Makefile, go.work module workspace."

echo ""
echo "=== Done ==="
git log --oneline -1
docker compose ps --format "table {{.Name}}\t{{.Status}}"
