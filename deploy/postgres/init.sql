-- Tabelas de destino dos sinks JDBC. A chave primária é a identidade da
-- janela (ou do alerta): o sink faz upsert, então reprocessar é idempotente.
CREATE TABLE IF NOT EXISTS greenhouse_window_aggregates (
    greenhouse_id TEXT             NOT NULL,
    sensor_type   TEXT             NOT NULL,
    window_start  TIMESTAMPTZ      NOT NULL,
    window_end    TIMESTAMPTZ      NOT NULL,
    sample_count  INTEGER          NOT NULL CHECK (sample_count > 0),
    min_value     DOUBLE PRECISION NOT NULL,
    max_value     DOUBLE PRECISION NOT NULL,
    mean_value    DOUBLE PRECISION NOT NULL,
    unit          TEXT             NOT NULL,
    PRIMARY KEY (greenhouse_id, sensor_type, window_start)
);

CREATE INDEX IF NOT EXISTS idx_aggregates_window_start ON greenhouse_window_aggregates (window_start);

CREATE TABLE IF NOT EXISTS greenhouse_alerts (
    alert_id       TEXT             PRIMARY KEY,
    greenhouse_id  TEXT             NOT NULL,
    sensor_type    TEXT             NOT NULL,
    window_start   TIMESTAMPTZ      NOT NULL,
    window_end     TIMESTAMPTZ      NOT NULL,
    rule           TEXT             NOT NULL,
    observed_value DOUBLE PRECISION NOT NULL,
    limit_value    DOUBLE PRECISION NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_alerts_greenhouse ON greenhouse_alerts (greenhouse_id, window_start);
