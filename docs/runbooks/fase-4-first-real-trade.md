# Fase 4 — First Real Trade Runbook

**Audiência:** operador humano (você).
**Quando usar:** após deploy completo da Fase 4 (T1-T7 commitados, container `polycopy-executor` rodando), antes de ativar real-mode pela primeira vez.

## ⚠️ Pré-requisito CRÍTICO de IP — Polymarket bloqueia datacenter

Polymarket usa Cloudflare WAF que **bloqueia rotas POST autenticadas** vindas de IPs de datacenter (Hetzner, AWS, GCP, Digital Ocean, OVH, Vultr, etc.). Sintomas confirmados em 2026-05-03 testando da Hetzner:

- `GET /markets` (read-only) → 200 OK
- `POST /auth/api-key` (`create_or_derive_api_creds`) → **403 Cloudflare** ("Sorry, you have been blocked")
- `POST /order` (`Web3CLOBExecutor.post_order`) → mesma classe de rota; bloqueado também (não testado, mas mesmo padrão)

**Implicação:** real-mode **NÃO funciona deste host** (Hetzner ou qualquer datacenter). Pra ativar real-mode, deployment precisa rodar de:

- **IP residencial** (laptop em casa, NAS doméstico, Raspberry Pi atrás do roteador) — solução recomendada e sem custo recorrente.
- **VPN com exit node residencial** (Mullvad, Proton, PIA — ~$5-15/mês) — funciona mas adiciona latência variável.

DRY-RUN funciona em qualquer IP (não chama POST autenticado) — pipeline coleta dados ininterruptamente em datacenter.

**Setup atual (2026-05-03):** wallet `0x3dE03D234E1931368B70fEce6c9387A734d938Df` configurada, fundada (MATIC + 24.79 USDC.e), allowance 100 USDC aprovada via tx [`0x5d533f...`](https://polygonscan.com/tx/0x5d533f323f6da16c766c34d6f2a2d003dafe42e18a170d5270c7ce6e651e3ba5). Real-mode bloqueado por IP datacenter — wallet pronta pra quando deployment migrar pra IP residencial.

## Pré-requisitos

- [x] `polycopy-executor` container rodando em DRY-RUN (default).
- [x] Pipeline upstream funcionando: watcher → risk → sizing → executor (logs limpos).
- [x] Conta Alchemy criada, `POLYGON_RPC_URL` no `.env`.
- [x] EOA criada (private key + address). Address fundada com:
  - **MATIC**: $5+ (gas)
  - **USDC**: $20-50 (collateral)

## Checklist

### Etapa 1: Setup wallet (one-shot)

```bash
# 1. Confirme .env tem WALLET_PRIVATE_KEY + POLYGON_RPC_URL
grep -E "WALLET_PRIVATE_KEY|POLYGON_RPC_URL" .env

# 2. Rode setup_wallet
uv run python -m polycopy.scripts.setup_wallet
```

**Output esperado:**
- Mostra address, balances MATIC/USDC, allowance atual.
- Pergunta `Approve $100 USDC for Exchange? (yes/no)`.
- Digite `yes`, confirma submissão da tx.
- Imprime tx_hash + URL Polygonscan.
- Aguarda confirmação on-chain (~2s em Polygon).
- Imprime `Approval confirmed on-chain`.

**Se falhar:** verifique gas (MATIC ≥ 0.1) + RPC URL válido.

### Etapa 2: Smoke opt-in (read-only)

```bash
PYTEST_LIVE_POLYGON=1 uv run pytest tests/integration/test_polymarket_smoke_executor.py -v
```

**Esperado:** 2 testes PASS (auth + allowance).

### Etapa 3: Validar pipeline DRY-RUN ainda saudável

```bash
docker compose logs --tail=100 executor | grep executor_decision
```

**Esperado:** logs com `mode=dry_run`, `result=dry_run` ou `failed` (sem real-mode ainda).

Aguarde 1h pra observar o pipeline em DRY-RUN sem erros novos.

### Etapa 4: Checkpoint git

```bash
git status     # confirme working tree limpo
git log -1     # confirme HEAD = T8 da Fase 4
```

### Etapa 5: Ativar real-mode (DOUBLE OPT-IN)

```bash
# Edita .env
sed -i 's/EXECUTOR_DRY_RUN=true/EXECUTOR_DRY_RUN=false/' .env
sed -i 's/EXECUTOR_REAL_MODE_CONFIRMED=false/EXECUTOR_REAL_MODE_CONFIRMED=true/' .env

# Restart executor (apenas)
docker compose restart executor

# Acompanhe logs
docker compose logs -f executor
```

**Esperado nos logs:**
- `agent_started`
- Sem `RuntimeError` (triple safety gates passaram).
- `verify_allowance` passou (sem erro de allowance).

### Etapa 6: Aguardar primeiro trade real

Pode levar minutos a horas (depende de wallets observadas + filtros do Risk + Sizing).

**Quando primeiro trade chegar:**
- Log: `executor_decision mode=real result=executed tx_hash=0x... gas_wei=...`.
- **Verifique a tx no Polygonscan**: `https://polygonscan.com/tx/0x...`.
- Confirme:
  - From = sua wallet address
  - To = Polymarket Exchange (`0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e`)
  - Value reflete trade.
- Verifique saldo USDC pós-trade: deve ter diminuído por `final_size_usdc`.

### Kill-switch operacional (caso algo errado)

Pause execução SEM reiniciar container:

```bash
mkdir -p /tmp/polycopy
touch /tmp/polycopy/executor.pause
```

Próximas tentativas vão retornar `MANUALLY_PAUSED`. Pra reativar:

```bash
rm /tmp/polycopy/executor.pause
```

Pra desativar real-mode completamente:

```bash
sed -i 's/EXECUTOR_DRY_RUN=false/EXECUTOR_DRY_RUN=true/' .env
docker compose restart executor
```

## Métricas a observar

```
http://127.0.0.1:9106/metrics
```

Procure por:
- `polycopy_executor_orders_total{mode="real"}` — devem aparecer após primeiro trade
- `polycopy_executor_kill_switch_blocks_total` — quantos foram bloqueados, por qual razão
- `polycopy_executor_consecutive_failures` — 0 = saudável, ≥3 = circuit breaker tripado
- `polycopy_executor_wallet_balance_usdc` — saldo atual

## Troubleshooting

| Sintoma | Causa provável | Ação |
|---|---|---|
| `RuntimeError: EXECUTOR_REAL_MODE_CONFIRMED required` | flag não setada | edita `.env` |
| `RuntimeError: WALLET_PRIVATE_KEY required` | chave não no `.env` | adiciona |
| `RuntimeError: USDC allowance insufficient` | setup_wallet não rodou | rode |
| Métrica `consecutive_failures ≥ 3` | RPC ou CLOB instável | investigue logs, pode `restart` container |
| `INSUFFICIENT_USDC_BALANCE` constante | wallet sem fundos | funda mais USDC |
| `INSUFFICIENT_USDC_ALLOWANCE` constante | allowance acabou | rode setup_wallet de novo (incrementa cap) |

## Quando parar

- Após primeiro trade real bem-sucedido + verificado no Polygonscan: você está em produção real-mode.
- Continue observando métricas pelos próximos dias.
- Se precisar aumentar `EXECUTOR_MAX_SIZE_USDC` ou outros caps: edite `.env`, restart, observe.
