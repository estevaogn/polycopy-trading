"""Shared test fixtures and bootstrap.

Carrega `.env` da raiz do repo se existir, antes dos testes coletarem env vars.
Em Task 6 essa lógica passa a usar `polycopy.config.Settings`; por ora, dotenv direto.
"""

from pathlib import Path

from dotenv import load_dotenv

_REPO_ROOT = Path(__file__).resolve().parent.parent
_ENV_FILE = _REPO_ROOT / ".env"

if _ENV_FILE.exists():
    load_dotenv(_ENV_FILE, override=False)
