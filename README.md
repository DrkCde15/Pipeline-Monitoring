# Monitoramento de Performance e Alertas

## Objetivo

Simular observabilidade de pipelines: registrar métricas de execução
(`pipeline_runs`), consultar histórico e disparar alertas por regras simples.

**Escopo desta versão (v0.1.0 — Etapa 1):** SQLite local + executor simulado
+ 4 regras puras e testáveis. Sem Airflow e sem Grafana ainda.

## Problema

Pipelines sem observabilidade falham em silêncio. Este projeto cria o
contrato mínimo: toda execução registra status, duração, linhas
(processadas/falha/inseridas/atualizadas) e erro; regras avaliam cada run.

## Arquitetura

```text
runner.simulate_run() → store (SQLite: pipeline_runs) → alerts.evaluate_run()
        │                                                        │
scripts/run_demo.py (4 cenários: success/slow/failed/empty) → alertas no stdout
```

Regras: `pipeline_failed`, `duration_exceeded`, `error_rate_exceeded`,
`rows_below_expected`. Limites via `.env` (`MAX_DURATION_SECONDS`,
`MAX_ERROR_RATE`, `MIN_ROWS_EXPECTED`).

Futuro: Airflow → PostgreSQL → Grafana; alertas por atraso (SLA) e
notificação (e-mail/Slack/webhook).

## Tecnologias

Python 3.10+ (stdlib `sqlite3`), python-dotenv, pytest. Nenhum serviço
externo.

## Estrutura do projeto

```text
pipeline-monitoring/
├── src/pipeline_monitoring/{config,store,alerts,runner}.py
├── scripts/run_demo.py
├── sql/                  # reservado: DDL Postgres futuro
├── data/                 # monitoring.db local (gitignored)
├── tests/test_monitoring.py
└── docs/architecture.md
```

## Como executar

```bash
cd pipeline-monitoring
pip install -e ".[dev]"
cp .env.example .env   # opcional
python scripts/run_demo.py
pytest
```

## Próximas etapas

1. Regra de atraso/SLA + heartbeat por pipeline.
2. Integração com pipelines reais (projetos 1–2) via decorator/contexto.
3. PostgreSQL + Airflow + Grafana (docker-compose local).
