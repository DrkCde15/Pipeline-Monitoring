# Arquitetura — Pipeline Monitoring

## Etapa 1 (implementada)

- `store.py`: ciclo `start_run → finish_run → fetch_runs` em SQLite.
- `alerts.py`: funções puras (sem I/O), thresholds injetáveis.
- `runner.py`: simulador com 4 cenários determinísticos para demo/testes.
- `sql/`: reservado para DDL Postgres (Airflow/Grafana futuros).

## Decisões

- Regras puras separadas do store: testabilidade sem banco.
- `duration_seconds` calculado de `started_at/finished_at` (ISO UTC).
- `stale_running`: runs ainda `running` são avaliados só por essa regra
  (métricas parciais não passam pelas 4 regras de run finalizado);
  relógio injetável (`now`) para testes determinísticos.
- `evaluate_run` rejeita status desconhecido e run finalizada sem
  `finished_at` (falha alto com ValueError, nunca silencioso).
- Catálogo `pipelines`: `start_run` exige registro prévio
  (`register_pipeline`, idempotente) para typo não fragmentar o
  histórico; `simulate_run` registra automaticamente por ser helper
  de demo/teste.
- `error_message` sanitizado no `finish_run` (máscara de credenciais +
  truncamento em 2000 chars): o monitor não vaza segredo via log.
- Demo com thresholds apertados (0.3s) para disparar alertas sempre.

## Futuro (não implementado)

SLA/atraso, notificação, decorator de instrumentação, Postgres, Airflow,
Grafana.
