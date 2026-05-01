# PolyCopy

Sistema multi-agente de copy trading na Polymarket com controle pelo celular. Observa carteiras lucrativas, copia trades de forma proporcional ao capital, aplica limites de risco automáticos e expõe controle remoto via Telegram. Operação 24/7 sem supervisão.

A arquitetura é hexagonal (Ports & Adapters) com 11 agentes Python independentes, cada um em container próprio, comunicando via NATS JetStream. O domínio é puro e testável; HTTP, banco e blockchain ficam isolados em adapters. Idempotência em qualquer fluxo que toca dinheiro, fail-safe por padrão (em dúvida, não opera), e tudo auditável a partir dos logs.

**Status atual:** Fase 0 — fundação. Projeto Python esqueleto, infraestrutura Docker (PostgreSQL+TimescaleDB, NATS, Redis, Prometheus), CI no GitHub Actions e tooling de qualidade (`ruff`, `mypy --strict`, `pytest`). Ainda sem código de domínio nem agentes. As fases seguintes estão catalogadas no `PROMPT_POLYCOPY_v2.md`.

## Requisitos

- Python 3.12.7 (fixado em `.python-version`)
- [uv](https://docs.astral.sh/uv/) (gerenciador de pacotes)
- Docker e Docker Compose v2.18+ (a partir do Passo 0.3)

## Setup local

```bash
uv sync
uv run pytest
```

## Bootstrap do `.env`

Antes de subir a infraestrutura Docker, gere o `.env` no servidor:

```bash
bash scripts/bootstrap-env.sh
```

O script copia `.env.example` para `.env`, gera uma senha forte para `POSTGRES_PASSWORD` via `openssl rand -hex 32` e aplica permissão `600`. Aborta se `.env` já existir — não sobrescreve.

## Onde o desenvolvimento acontece

O desenvolvimento é feito **no servidor** via SSH (`polycopy@178.105.46.37`), na pasta `~/projects/polycopy`. O ambiente local (Windows + PowerShell) é usado apenas para conexão SSH; nada de Docker local.

## Referência

A estrutura completa do projeto (todas as fases até a 7) está documentada no Apêndice A do `PROMPT_POLYCOPY_v2.md`. O catálogo de eventos NATS, tabelas e métricas Prometheus também vive lá.
