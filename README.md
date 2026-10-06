# Monitoramento de Performance e Alertas

## Objetivo

Simular observabilidade de pipelines: registrar métricas de execução
(`pipeline_runs`), consultar histórico e disparar alertas por regras simples.
Pipelines têm catálogo (`pipelines`): nome fora do registro falha alto
em vez de fragmentar o histórico com typos.

**Escopo desta versão (v0.1.0 — Etapa 1):** SQLite local + executor simulado
+ 5 regras puras e testáveis + catálogo de pipelines + CI.
Sem Airflow e sem Grafana ainda.

## Problema

Pipelines sem observabilidade falham em silêncio. Este projeto cria o
contrato mínimo: toda execução registra status, duração, linhas
(processadas/falha/inseridas/atualizadas) e erro; regras avaliam cada run.

## Arquitetura

```text
register_pipeline() → runner.simulate_run()/store.start_run →
    store (SQLite: pipeline_runs + pipelines) → alerts.evaluate_run()
        │                                                  │
scripts/run_demo.py (4 cenários: success/slow/failed/empty) → alertas no stdout
```

Regras: `pipeline_failed`, `duration_exceeded`, `error_rate_exceeded`,
`rows_below_expected` (só runs finalizados) e `stale_running` (run preso
em `running` além de `MAX_RUNNING_SECONDS`, ou sem `started_at` válido).
Limites via `.env` (`MAX_DURATION_SECONDS`, `MAX_ERROR_RATE`,
`MIN_ROWS_EXPECTED`, `MAX_RUNNING_SECONDS`).

Contratos que falham alto (`ValueError`, nunca silencioso): `start_run`
exige pipeline registrada; `evaluate_run` rejeita status desconhecido
e run finalizada sem `finished_at`.

Futuro: Airflow → PostgreSQL → Grafana; alertas por atraso (SLA) e
notificação (e-mail/Slack/webhook).

## Tecnologias

Python 3.10+ (stdlib `sqlite3`), python-dotenv, pytest. Nenhum serviço
externo.

## Estrutura do projeto

```text
pipeline-monitoring/
├── src/pipeline_monitoring/{config,store,alerts,runner,instrument}.py
├── scripts/run_demo.py
├── sql/                  # reservado: DDL Postgres futuro
├── data/                 # monitoring.db local (gitignored)
├── tests/test_monitoring.py   # 14 testes: regras + ciclo de vida + catálogo
├── .github/workflows/ci.yml   # install + pytest em push/PR
└── docs/{architecture,revisao-engenharia-dados}.md
```

## Como executar

```bash
cd pipeline-monitoring
pip install -e ".[dev]"
cp .env.example .env   # opcional
python scripts/run_demo.py
pytest
```

## Uso em pipelines reais

```python
from pathlib import Path
from pipeline_monitoring.instrument import monitored_run

db = Path("data/monitoring.db")
with monitored_run(db, "minha_pipeline") as run:
    ...  # ETL real aqui
    run.rows_processed = n
    run.rows_inserted = n
# saída do bloco: success com métricas; exceção: failed + relançada.
# Alertas avaliados, logados e em `run.alerts`.
```

Para `main()` de script, há o decorator `@monitor` (métricas via dict
retornado). No Airflow, prefira os callbacks `on_task_success` /
`on_task_failure` de `dags/monitoring_callbacks.py` (já wired nos
projetos `financial-data-pipeline` e `multi-format-etl`): uma run por
task com início/fim reais, sem mudar os scripts ETL.

Exemplo manual (sem o context manager):

```python
from pathlib import Path
from pipeline_monitoring.store import (
    finish_run, init_db, register_pipeline, start_run,
)

db = Path("data/monitoring.db")
init_db(db)
register_pipeline(db, "minha_pipeline")  # uma vez; nome fora do catálogo falha alto

run = start_run(db, "minha_pipeline")
try:
    ...  # ETL real aqui
except Exception as exc:
    finish_run(db, run, status="failed", error_message=str(exc))
    raise
else:
    finish_run(db, run, status="success", rows_processed=n, rows_inserted=n)
```

## Próximas etapas

1. Heartbeat/SLA completo (`stale_running` cobre run presa; falta atraso de schedule).
2. Thresholds por pipeline.
3. Volume por task nos projetos integrados (linhas via `monitored_run` nos scripts).
4. PostgreSQL + Grafana (docker-compose local).
