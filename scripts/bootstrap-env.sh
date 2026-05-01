#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ROOT}/.env"
EXAMPLE_FILE="${ROOT}/.env.example"

if [[ -f "${ENV_FILE}" ]]; then
  echo "ERROR: ${ENV_FILE} already exists. Refusing to overwrite." >&2
  exit 1
fi

if [[ ! -f "${EXAMPLE_FILE}" ]]; then
  echo "ERROR: ${EXAMPLE_FILE} not found." >&2
  exit 1
fi

cp "${EXAMPLE_FILE}" "${ENV_FILE}"
PASSWORD="$(openssl rand -hex 32)"
sed -i "s|^POSTGRES_PASSWORD=.*|POSTGRES_PASSWORD=${PASSWORD}|" "${ENV_FILE}"
chmod 600 "${ENV_FILE}"
echo "Created ${ENV_FILE} with permission 600."
