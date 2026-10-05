# Arquitetura — Pipeline Monitoring

## Etapa 1 (implementada)

- `store.py`: ciclo `start_run → finish_run → fetch_runs` em SQLite.
- `alerts.py`: funções puras (sem I/O), thresholds injetáveis.
- `runner.py`: simulador com 4 cenários determinísticos para demo/testes.
- `sql/`: reservado para DDL Postgres (Airflow/Grafana futuros).

## Decisões

- Regras puras separadas do store: testabilidade sem banco.
- `duration_seconds` calculado de `started_at/finished_at` (ISO UTC).
- Demo com thresholds apertados (0.3s) para disparar alertas sempre.

## Futuro (não implementado)

SLA/atraso, notificação, decorator de instrumentação, Postgres, Airflow,
Grafana.
