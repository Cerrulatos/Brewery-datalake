-- Cria schema audit
CREATE SCHEMA IF NOT EXISTS audit;

-- Tabela pai: dag_runs
CREATE TABLE IF NOT EXISTS audit.dag_runs (
  dag_id            TEXT NOT NULL,
  run_id            TEXT NOT NULL,
  status            TEXT NOT NULL,
  execution_date    TIMESTAMPTZ NULL,
  logical_date      TIMESTAMPTZ NULL,
  start_time        TIMESTAMPTZ NULL,
  end_time          TIMESTAMPTZ NULL,
  duration_seconds  DOUBLE PRECISION NULL,
  triggered_by      TEXT NULL,
  host              TEXT NULL,
  metrics           JSONB NOT NULL DEFAULT '{}'::jsonb,
  error             TEXT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT dag_runs_pk PRIMARY KEY (dag_id, run_id)
);

-- Índices úteis
CREATE INDEX IF NOT EXISTS idx_dag_runs_status ON audit.dag_runs(status);
CREATE INDEX IF NOT EXISTS idx_dag_runs_exec_date ON audit.dag_runs(execution_date);

-- Tabela filha: task_events
CREATE TABLE IF NOT EXISTS audit.task_events (
  id          BIGSERIAL PRIMARY KEY,
  dag_id      TEXT NOT NULL,
  run_id      TEXT NOT NULL,
  task_id     TEXT NOT NULL,
  status      TEXT NOT NULL,
  message     TEXT NULL,
  metrics     JSONB NOT NULL DEFAULT '{}'::jsonb,
  try_number  INTEGER NULL,
  map_index   INTEGER NULL,
  log_url     TEXT NULL,
  event_time  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT task_events_fk
    FOREIGN KEY (dag_id, run_id)
    REFERENCES audit.dag_runs(dag_id, run_id)
    ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_task_events_dag_run ON audit.task_events(dag_id, run_id);
CREATE INDEX IF NOT EXISTS idx_task_events_task_id ON audit.task_events(task_id);
CREATE INDEX IF NOT EXISTS idx_task_events_event_time ON audit.task_events(event_time);
