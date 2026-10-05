# Revisão de Engenharia de Dados — pipeline-monitoring

**Data:** 2026-10-05
**Escopo:** repositório local `pipeline-monitoring` (README, `src/pipeline_monitoring/*`, `tests/`, `scripts/run_demo.py`, `docs/architecture.md`, `.env.example`, `pyproject.toml`; `sql/` vazio, `data/monitoring.db` local). Sem histórico git — o diretório não é um repo git, então o inegociável "segredos no histórico" não pôde ser verificado.
**Maturidade assumida:** Estágio 1 — protótipo de portfólio que declara explicitamente "v0.1.0 — Etapa 1: SQLite local + executor simulado, sem Airflow/Grafana ainda" (`README.md:8`).

## Veredito

É um protótipo honesto de Estágio 1: escopo mínimo declarado, sem over-engineering, com a melhor decisão do projeto já tomada — regras de alerta como funções puras separadas do store (`src/pipeline_monitoring/alerts.py:25`). Para o propósito (portfólio/estudo de observabilidade) está no caminho certo. O que mais importa agora: (1) runs `running` órfãos são invisíveis às 4 regras — o "monitoramento" não detecta o pipeline que morre sem avisar; (2) `evaluate_run` sem guarda de status gera falso-positivo em runs incompletos; (3) `pipeline_name` sem registro fragmenta o histórico com um typo. Nenhum achado crítico (sem segredo, sem PII, sem corrupção silenciosa confirmada).

## Mapa do ciclo de vida

| Etapa do ciclo | Onde está no repo | Tecnologia | Observação |
|---|---|---|---|
| Geração (fontes) | `src/pipeline_monitoring/runner.py` | Simulador (4 cenários) | Sem fonte real — assumido no estágio; sem contrato/schema |
| Armazenamento | `src/pipeline_monitoring/store.py:20-34`, `data/monitoring.db` | SQLite, tabela única `pipeline_runs` | Camada única (raw = curado); `sql/` reservado, vazio |
| Ingestão | `store.py:67-124` (`start_run`/`finish_run`) | Escrita síncrona direta | Sem retry, validação, checkpoint ou reconciliação |
| Transformação | `src/pipeline_monitoring/alerts.py` | 4 regras puras | Sem modelagem dimensional — adequado ao domínio |
| Disponibilização | `scripts/run_demo.py`, `store.fetch_runs` | stdout + consulta SQLite | Sem BI, API, frescor visível ou notificação (futuro declarado) |

## Scorecard

| Dimensão | Nota (0–3) | Esperado no estágio | Resumo em uma linha |
|---|---|---|---|
| Geração | 1 | 1 | Simulador determinístico honesto, sem fonte real |
| Armazenamento | 2 | 1–2 | `init_db` idempotente, SQLite stdlib; sem migração/índice |
| Ingestão | 1 | 1 | Funciona, mas run interrompido vira `running` eterno |
| Transformação | 2 | 1–2 | Regras puras, testadas, thresholds injetáveis |
| Disponibilização | 1 | 1 | Demo no stdout; sem frescor, dicionário ou acesso |
| Segurança/privacidade | 2 | 1–2 | Sem segredos/PII; `error_message` livre é o único risco futuro |
| Gerenciamento | 1 | 1 | README + docstrings; sem catálogo nem testes de dados |
| DataOps | 1 | 1 | 6 testes que passam; sem CI, sem monitoramento do monitor |
| Arquitetura | 2 | 1–2 | Sóbria e reversível; sem over-engineering |
| Orquestração | 0 | 0–1 | Ausente por design (futuro Airflow declarado) |
| Eng. de software | 2 | 1–2 | Modular e tipado; `pytest` quebra sem install, `sys.path` hack na demo |

## Pontos fortes verificados

- Regras puras sem I/O, testáveis isoladamente (`alerts.py:25`, `tests/test_monitoring.py:27-52`).
- Timestamps sempre com fuso explícito UTC (`store.py:72,101`) — sem fuso implícito.
- `error_rate` com guarda `processed > 0` (`alerts.py:45`) — sem falso-positivo de divisão por zero.
- `.env` nunca versionado, só `.env.example` sem segredos (`ls` confirma: sem `.env` local); `data/*.db` no `.gitignore:18-19`.
- Escopo contido e futuro declarado em vez de construído antes da hora (`README.md:59-63`, `docs/architecture.md:16-19`).

## Achados

### H1 Runs `running` órfãos invisíveis às regras — Alta · Esforço P
- **Local:** `src/pipeline_monitoring/store.py:67-83` + `src/pipeline_monitoring/alerts.py:25-55`
- **Status:** verificado
- **Evidência:** `start_run` insere `status='running'`; se o processo morre antes de `finish_run`, a linha fica `running` para sempre. Nenhuma das 4 regras casa com `status='running'` (só `failed`, duração, error rate e volume — e run incompleto tem duração nula). O SLA/heartbeat está listado como "próxima etapa" no README, então é lacuna conhecida, mas é exatamente o cenário "pipeline que falha em silêncio" que o projeto diz combater (`README.md:13-15`).
- **Por que importa:** observabilidade que não detecta morte sem aviso entrega confiança falsa (corrente DataOps: frescor/falhas sem alerta).
- **Como corrigir:** regra `stale_running` (ex.: `started_at` há mais de N min sem `finished_at`) + teste; depois heartbeat/SLA como já planejado.

### H2 `evaluate_run` avalia runs incompletos — Alta · Esforço P
- **Local:** `src/pipeline_monitoring/alerts.py:25-54`
- **Status:** verificado
- **Evidência:** função aceita qualquer dict, sem exigir `status in ('success','failed')` ou `finished_at` preenchido. Um run ainda `running` tem `rows_processed=0` e dispara `rows_below_expected` indevidamente.
- **Por que importa:** erro silencioso invertido — alerta falso que corrói confiança, ou avaliação prematura tratada como verdade (transformação sem grão de "run finalizado" declarado).
- **Como corrigir:** retornar `[]` (ou levantar `ValueError`) quando `finished_at` ausente/`status == 'running'`; documentar "só avalie runs finalizados"; teste com dict `running`.

### M1 `pipeline_name` sem registro — typo cria pipeline fantasma — Média · Esforço P
- **Local:** `src/pipeline_monitoring/store.py:67,127-134`
- **Status:** verificado
- **Evidência:** `pipeline_name` é string livre, sem validação, enum ou tabela de registro; `fetch_runs` filtra por igualdade exata. `bronze_ingestion` vs `bronze-ingestion` viram dois históricos.
- **Por que importa:** fragmentação silenciosa do histórico — o dashboard mostra "sem dados" em vez de erro (gerenciamento: sem catálogo/dono).
- **Como corrigir:** validar contra lista de pipelines conhecidos (argumento ou tabela `pipelines`) ou ao menos `strip()/lower()` + teste.

### M2 Thresholds definidos em dois lugares — Média · Esforço P
- **Local:** `scripts/run_demo.py:27-29` vs `.env.example:4-6` + `src/pipeline_monitoring/config.py:68-72`
- **Status:** verificado
- **Evidência:** demo fixa `max_duration_seconds=0.3` hardcoded enquanto o default real é `300`. O mesmo KPI tem dois valores conforme o caminho de execução.
- **Por que importa:** "o mesmo número bate entre dashboards?" (disponibilização) — aqui nem entre demo e produção.
- **Como corrigir:** demo usa `load_settings().thresholds` com override só via env/flag explícita.

### M3 Schema sem migração nem índice — Média · Esforço M
- **Local:** `src/pipeline_monitoring/store.py:20-34,127-139`
- **Status:** verificado
- **Evidência:** evolução via `CREATE TABLE IF NOT EXISTS` — coluna nova exige intervenção manual; `fetch_runs` faz `SELECT * ... ORDER BY started_at DESC` sem índice em `(pipeline_name, started_at)`; `SELECT *` acopla `evaluate_run` ao schema físico.
- **Por que importa:** fragilidade que só aparece na escala/manutenção (armazenamento: retenção/evolução).
- **Como corrigir:** quando migrar ao Postgres (`sql/`): DDL versionado, índice composto, colunas explícitas em vez de `*`. No SQLite atual, nada urgente.

### M4 `pytest` quebra sem `pip install -e .` — Média · Esforço P
- **Local:** `pyproject.toml:16-17`, `scripts/run_demo.py:12`, `tests/test_monitoring.py:7-10`
- **Status:** verificado (testado: `pytest` → `ModuleNotFoundError`; com `PYTHONPATH=src` os 6 testes passam.)
- **Evidência:** pacote só importável instalado; a demo contorna com `sys.path.insert`.
- **Por que importa:** inegociável nº 5 — "outra pessoa consegue rodar seguindo o README" vale só se o `pip install` funcionar; sem CI ninguém percebe a quebra.
- **Como corrigir:** remover o `sys.path.insert` da demo (usar o pacote instalado) e adicionar CI mínimo (`.github/workflows/ci.yml` com `pip install -e ".[dev]" && pytest`).

### M5 `error_message` texto livre sem mascaramento — Média · Esforço P
- **Local:** `src/pipeline_monitoring/store.py:95-116`, `scripts/run_demo.py:37`
- **Status:** inferido (hoje o conteúdo é simulado: `runner.py:39`)
- **Evidência:** campo persistido e impresso no stdout sem sanitização. Quando instrumentar pipelines reais (roadmap item 2), tracebacks podem carregar connection strings ou PII.
- **Por que importa:** segurança/privacidade + "pensamento negativo": o monitor vira vetor de vazamento (LGPD — acréscimo desta skill, não do livro).
- **Como corrigir:** truncar + máscara simples de padrões (`senha=`, `://user:pass@`) antes de persistir; documentar "nunca logar payload".

### B1 `run_id` truncado e `finish_run` duplo silencioso — Baixa · Esforço P
- **Local:** `src/pipeline_monitoring/store.py:71,86-124`
- **Status:** verificado
- **Evidência:** `uuid4().hex[:12]` (48 bits, sem checagem de colisão); chamar `finish_run` duas vezes sobrescreve métricas sem aviso.
- **Por que importa:** sem impacto visível hoje; vira problema com concorrência.
- **Como corrigir:** `hex` completo + `UPDATE ... WHERE status='running'` com `rowcount` checado.

## Roadmap

1. **Agora** — H1 (regra `stale_running` + teste), H2 (guarda de status em `evaluate_run`), M4 (CI mínimo + remover `sys.path` hack). Tudo P, fecha os erros silenciosos e o "roda do zero".
2. **Em seguida** — M1 (registro de pipelines), M2 (threshold único via settings), M5 (sanitizar `error_message`), itens já planejados do README: SLA/heartbeat, decorator de instrumentação nos projetos 1–2, Postgres + índice (M3).
3. **Deliberadamente adiado** — Airflow/Grafana/docker-compose, catálogo de dados, DataOps completo (retries, dead-letter, backfill nativo), particionamento/retenção quente-morno-frio: seria over-engineering para 4 runs simulados em SQLite. O livro manda pesar complexidade contra valor — aqui o valor ainda é aprender o contrato mínimo.

## O que esta análise não cobriu

Produção e volumes reais (só há runs simulados locais), custos, permissões na nuvem (nada aqui sai da máquina), qualidade real dos dados (fonte é fabricada), histórico do git (diretório não versionado — segredos no histórico não verificáveis) e o que vive fora do repo (futuros Airflow/Grafana/BI).

## Perguntas em aberto

1. Este projeto vai instrumentar pipelines reais (quais?) ou permanecer como biblioteca de demonstração? Isso decide se H1/M5 são urgentes ou teóricos.
2. Qual o destino do `monitoring.db` — SQLite é permanente para este projeto ou ponte até o Postgres? Isso decide o investimento em M3.
3. Há algum consumidor além do stdout da demo (outra pessoa, dashboard, avaliação de portfólio automatizada)? Isso calibra o peso de M2/M4.
