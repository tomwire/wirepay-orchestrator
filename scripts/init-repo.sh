#!/usr/bin/env bash
# Git initialization with GPG verification for wirepay-orchestrator.
# Run this ONCE from the repo root:  ./scripts/init-repo.sh
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GPG_KEY_ID="823F6A19A409AFE0"
USER_NAME="Thomas Wire"
USER_EMAIL="tom@thomaswire.com"

echo "==> Initializing git repo at $REPO_ROOT"
cd "$REPO_ROOT"

git init -b main
git config user.name "$USER_NAME"
git config user.email "$USER_EMAIL"
git config commit.gpgsign true
git config gpg.format ssh || true   # keep openclaw-configured default (ed25519)

echo "==> Generating go.sum..."
go mod tidy

echo "==> Staging all files..."
git add -A

echo "==> Initial commit (GPG-signed)..."
GIT_AUTHOR_NAME="$USER_NAME" \
GIT_AUTHOR_EMAIL="$USER_EMAIL" \
GIT_COMMITTER_NAME="$USER_NAME" \
GIT_COMMITTER_EMAIL="$USER_EMAIL" \
git commit -S --gpg-sign="tom@thomaswire.com" \
  -m "feat: initial WPO scaffold — 5 microservices, docker-compose dev stack

- wire-order: checkout saga orchestrator (state-machine, idempotent)
- wire-inventory: stock reservation with Redlock + SELECT FOR UPDATE
- wire-payment: Stripe mock/real provider, idempotency tokens
- wire-notification: templated notifications with dead-letter queue
- wire-ledger: double-entry bookkeeping with auto balance updates

Infrastructure: Dockerfile (multi-stage), docker-compose.yml (PG, Redis, Kafka),
Makefile, go.work module workspace."

echo "==> Done. Status:"
git log --oneline -1
git status -s | head -20
