import { execFileSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { expect, test, type APIRequestContext, type Page } from '@playwright/test';
import { Client } from 'pg';
import { narrar } from './narrar';

const env = (k: string, d: string) => process.env[k] ?? d;
const REGISTRY = env('SCHEMA_REGISTRY_URL', 'http://localhost:8081');
const CONNECT = env('CONNECT_URL', 'http://localhost:8083');
const KAFKA_UI = env('KAFKA_UI_URL', 'http://localhost:8080');
const AGGREGATOR = env('AGGREGATOR_URL', 'http://localhost:9102');
const PROMETHEUS = env('PROMETHEUS_URL', 'http://localhost:9090');
const MINIO_CONSOLE = env('MINIO_CONSOLE_URL', 'http://localhost:9001');
const PG_URL = env('PG_URL', 'postgres://telemetry:dev-only-change-me@localhost:5432/telemetry');
const CLUSTER = 'agrovale-local';
const ROOT = path.resolve(__dirname, '../..');
const DEMO = `gh-demo-${Date.now().toString(36)}`;

const compose = (...args: string[]) =>
  execFileSync('docker', ['compose', '-p', 'kafka-telemetry', ...args], {
    cwd: ROOT,
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
  });

async function sql(query: string, params: unknown[] = []) {
  const client = new Client({ connectionString: PG_URL });
  await client.connect();
  try {
    return (await client.query(query, params)).rows;
  } finally {
    await client.end();
  }
}

function tabela(rows: Record<string, unknown>[]) {
  if (rows.length === 0) return '(0 linhas)';
  const cols = Object.keys(rows[0]);
  const fmt = (v: unknown) => (v instanceof Date ? v.toISOString().slice(0, 19) + 'Z' : String(v));
  const width = cols.map((c) => Math.max(c.length, ...rows.map((r) => fmt(r[c]).length)));
  const line = (vals: string[]) => vals.map((v, i) => v.padEnd(width[i])).join(' | ');
  return [line(cols), width.map((w) => '-'.repeat(w)).join('-+-'), ...rows.map((r) => line(cols.map((c) => fmt(r[c]))))].join('\n');
}

// murmur2 do particionador padrão do Kafka (o mesmo do franz-go): a Kafka UI
// exige a partição explícita, e a deduplicação só vale dentro da partição da estufa.
function murmur2(data: Buffer): number {
  const m = 0x5bd1e995;
  let h = (0x9747b28c ^ data.length) | 0;
  const n4 = data.length >> 2;
  for (let i = 0; i < n4; i++) {
    let k = data.readInt32LE(i * 4);
    k = Math.imul(k, m);
    k ^= k >>> 24;
    k = Math.imul(k, m);
    h = Math.imul(h, m) ^ k;
  }
  const rest = data.length & 3;
  const off = n4 * 4;
  if (rest === 3) h ^= data[off + 2] << 16;
  if (rest >= 2) h ^= data[off + 1] << 8;
  if (rest >= 1) {
    h ^= data[off];
    h = Math.imul(h, m);
  }
  h ^= h >>> 13;
  h = Math.imul(h, m);
  h ^= h >>> 15;
  return h;
}

const particaoDa = (key: string, partitions: number) => (murmur2(Buffer.from(key)) & 0x7fffffff) % partitions;

// Produz pela Kafka UI, que serializa em Avro com o schema mais recente do subject.
async function produzir(request: APIRequestContext, topic: string, key: string, content: string, valueSerde = 'SchemaRegistry') {
  const topicUrl = `${KAFKA_UI}/api/clusters/${CLUSTER}/topics/${topic}`;
  const info = await (await request.get(topicUrl)).json();
  const partition = particaoDa(key, info.partitions.length);
  const url = `${topicUrl}/messages`;
  const resp = await request.post(url, { data: { key, content, partition, keySerde: 'String', valueSerde } });
  expect(resp.status(), await resp.text()).toBe(200);
  return { metodo: 'POST', url, status: resp.status(), partition };
}

async function metricas(request: APIRequestContext) {
  const resp = await request.get(`${AGGREGATOR}/metrics`);
  expect(resp.status()).toBe(200);
  return (await resp.text()).split('\n').filter((l) => l.startsWith('telemetry_'));
}

const soma = (linhas: string[], prefixo: string) =>
  linhas.filter((l) => l.startsWith(prefixo)).reduce((acc, l) => acc + Number(l.split(' ').pop()), 0);

async function abrirTopico(page: Page, topic: string, filtro?: string) {
  const q = new URLSearchParams({
    seekType: 'LATEST', seekDirection: 'BACKWARD', limit: '100', page: '0', keySerde: 'String', valueSerde: 'SchemaRegistry',
  });
  if (filtro) {
    q.set('filterQueryType', 'STRING_CONTAINS');
    q.set('q', filtro);
  }
  await page.goto(`${KAFKA_UI}/ui/clusters/${CLUSTER}/all-topics/${topic}/messages?${q}`);
  await expect(page.locator('table tbody tr', { hasText: filtro ?? '' }).first()).toBeVisible({ timeout: 30_000 });
}

test('kafka-telemetry: fluxo principal ponta a ponta', async ({ page, request }) => {
  await narrar(page, 'kafka-telemetry: evidência do fluxo principal', {
    descricao: `Pipeline de telemetria das estufas da cooperativa fictícia AgroVale Exemplo.
Kafka KRaft + Schema Registry (Avro, BACKWARD) + agregador Go + Kafka Connect (Postgres e MinIO) + Redis.
Vamos ver: tópicos e schemas, leituras particionadas por estufa, janelas de 1 minuto, alerta, DLQ,
deduplicação, último estado no Redis, schema incompatível recusado, métricas e Parquet no data lake.
Data: ${new Date().toISOString().slice(0, 16).replace('T', ' ')} UTC. Estufa de teste: ${DEMO}.`,
  }, 4000);

  // 1. telemetryctl init
  const init = compose('run', '--rm', '--no-deps', 'init');
  const subjectsResp = await request.get(`${REGISTRY}/subjects`);
  expect(subjectsResp.status()).toBe(200);
  const subjects: string[] = await subjectsResp.json();
  expect(subjects).toEqual(expect.arrayContaining(['greenhouse.readings-value', 'greenhouse.aggregates-value', 'greenhouse.alerts-value']));
  const config = await (await request.get(`${REGISTRY}/config/greenhouse.readings-value`)).json();
  expect(config.compatibilityLevel).toBe('BACKWARD');
  await narrar(page, '1. telemetryctl cria tópicos e registra os schemas', {
    descricao: 'docker compose run init → telemetryctl init (idempotente). Saída real e subjects no Schema Registry:',
    saida: init.split('\n').filter((l) => l.trim()).slice(-8).map((l) => l.slice(0, 170)).join('\n') +
      `\n\nGET ${REGISTRY}/subjects → ${JSON.stringify(subjects)}\nGET /config/greenhouse.readings-value → ${JSON.stringify(config)}`,
  }, 4500);
  await page.goto(`${KAFKA_UI}/ui/clusters/${CLUSTER}/schemas`);
  const linhaSchema = page.getByRole('link', { name: 'greenhouse.readings-value' });
  await expect(linhaSchema).toBeVisible({ timeout: 30_000 });
  await linhaSchema.highlight();
  await page.waitForTimeout(2500);
  await linhaSchema.click();
  await expect(page.getByText('BACKWARD').first()).toBeVisible();
  await page.getByText('BACKWARD').first().highlight();
  await page.waitForTimeout(3000);

  // Leituras controladas da estufa de teste: duas válidas acima do limite (32 °C),
  // uma retransmissão da segunda (mesmo event_id), uma fora da faixa física e uma que nem é Avro.
  const agora = Date.now();
  const t1 = Math.floor(agora / 60000) === Math.floor((agora - 1000) / 60000) ? agora - 1000 : agora;
  const leitura = (eventId: string, value: number, measuredAt: number, type = 'TEMPERATURE', unit = 'celsius') =>
    JSON.stringify({
      event_id: eventId, greenhouse_id: DEMO, sensor_id: `${DEMO}-${type.toLowerCase()}`, sensor_type: type,
      value, unit, measured_at: measuredAt, firmware_version: { string: '1.4.0' },
    });
  const idA = randomUUID();
  const idB = randomUUID();
  const enviadas = [
    await produzir(request, 'greenhouse.readings', DEMO, leitura(idA, 44.2, t1)),
    await produzir(request, 'greenhouse.readings', DEMO, leitura(idB, 45.8, agora)),
    await produzir(request, 'greenhouse.readings', DEMO, leitura(idB, 45.8, agora)),
    await produzir(request, 'greenhouse.readings', DEMO, leitura(randomUUID(), 140.5, agora, 'HUMIDITY', 'percent')),
    await produzir(request, 'greenhouse.readings', DEMO, '{"isto":"não é avro"}', 'String'),
  ];
  await narrar(page, 'Preparação: leituras controladas da estufa de teste', {
    descricao: `Publicadas via Kafka UI (Avro pelo Schema Registry), chave = ${DEMO}:
• temperatura 44,2 °C (event_id ${idA.slice(0, 8)}…) e 45,8 °C (event_id ${idB.slice(0, 8)}…)
• retransmissão da leitura de 45,8 °C com o MESMO event_id
• umidade 140,5% (fora da faixa física) e uma mensagem que não é Avro
Se a duplicata contasse, a média seria 45,27; o esperado é 45,00 com 2 amostras.`,
    chamada: { ...enviadas[0], corpo: enviadas.map((e, i) => `#${i + 1} ${e.metodo} partição ${e.partition} → ${e.status}`).join('\n') },
  }, 4500);

  // 2. Simulador produzindo, particionado por estufa
  await narrar(page, '2. O simulador produz leituras de várias estufas', {
    descricao: 'Produtor idempotente (acks=all), chave = greenhouse_id: todas as leituras de uma estufa caem na mesma partição, o que mantém a ordem por estufa.',
  });
  await page.goto(`${KAFKA_UI}/ui/clusters/${CLUSTER}/all-topics/greenhouse.readings`);
  await expect(page.getByText('Partitions').first()).toBeVisible({ timeout: 30_000 });
  await page.waitForTimeout(2500);
  await abrirTopico(page, 'greenhouse.readings');
  await page.locator('table thead th', { hasText: 'Partition' }).first().highlight();
  await page.waitForTimeout(3000);
  await abrirTopico(page, 'greenhouse.readings', 'gh-007');
  await page.locator('table thead th', { hasText: 'Partition' }).first().highlight();
  const linhas = await page.locator('table tbody tr', { hasText: 'gh-007' }).allInnerTexts();
  const particoes = new Set(linhas.map((l) => l.trim().split(/\s+/)[1]));
  expect(particoes.size, `partições da gh-007: ${[...particoes]}`).toBe(1);
  await page.waitForTimeout(3000);

  // 3. Agregados no tópico e no Postgres
  await narrar(page, '3. O agregador fecha janelas de 1 minuto', {
    descricao: 'Janelas tumbling por estufa e grandeza em tempo de evento. Os agregados vão para greenhouse.aggregates e o Kafka Connect (sink JDBC, upsert) grava no Postgres.',
  });
  await abrirTopico(page, 'greenhouse.aggregates');
  await page.waitForTimeout(3000);
  await page.goto(`${KAFKA_UI}/ui/clusters/${CLUSTER}/connectors`);
  await expect(page.getByText('greenhouse-aggregates-jdbc-sink')).toBeVisible({ timeout: 30_000 });
  await page.getByText('greenhouse-aggregates-jdbc-sink').highlight();
  await page.waitForTimeout(3000);
  const connectors = await (await request.get(`${CONNECT}/connectors?expand=status`)).json();
  const estados = Object.fromEntries(Object.entries(connectors).map(([k, v]: [string, any]) => [k, v.status.connector.state]));
  expect(estados['greenhouse-aggregates-jdbc-sink']).toBe('RUNNING');
  let ultimos: Record<string, unknown>[] = [];
  await expect.poll(async () => {
    ultimos = await sql(`SELECT greenhouse_id, sensor_type, window_start, sample_count, round(mean_value::numeric, 2) AS media
      FROM greenhouse_window_aggregates ORDER BY window_start DESC, greenhouse_id LIMIT 8`);
    return ultimos.length;
  }, { timeout: 150_000, intervals: [5000] }).toBeGreaterThan(0);
  await narrar(page, '3. Agregados no Postgres (consulta real)', {
    descricao: `Conectores: ${JSON.stringify(estados)}`,
    saida: `SELECT greenhouse_id, sensor_type, window_start, sample_count, media\nFROM greenhouse_window_aggregates ORDER BY window_start DESC LIMIT 8;\n\n${tabela(ultimos)}`,
  }, 4500);

  // 4 e 6. Espera a janela da estufa de teste fechar e chegar ao Postgres
  let demoRows: Record<string, any>[] = [];
  await expect.poll(async () => {
    demoRows = await sql(`SELECT greenhouse_id, sensor_type, window_start, sample_count, min_value, max_value,
      round(mean_value::numeric, 2) AS media FROM greenhouse_window_aggregates WHERE greenhouse_id = $1`, [DEMO]);
    return demoRows.length;
  }, { timeout: 150_000, intervals: [5000] }).toBeGreaterThan(0);

  await narrar(page, '4. Estufa superaquecida gera alerta', {
    descricao: `A média da janela da ${DEMO} (45 °C) passa do limite de 32 °C: o agregador publica em greenhouse.alerts.`,
  });
  await abrirTopico(page, 'greenhouse.alerts', DEMO);
  await page.locator('table tbody tr', { hasText: DEMO }).first().highlight();
  await page.waitForTimeout(3000);
  let alertas: Record<string, unknown>[] = [];
  await expect.poll(async () => {
    alertas = await sql(`SELECT greenhouse_id, sensor_type, rule, observed_value, limit_value, window_start
      FROM greenhouse_alerts WHERE greenhouse_id = $1`, [DEMO]);
    return alertas.length;
  }, { timeout: 60_000, intervals: [3000] }).toBe(1);
  expect(alertas[0].rule).toBe('ABOVE_MAX');
  await narrar(page, '4. Alerta também no Postgres (consulta real)', {
    descricao: 'O sink JDBC de alertas grava com upsert por alert_id.',
    saida: `SELECT ... FROM greenhouse_alerts WHERE greenhouse_id = '${DEMO}';\n\n${tabela(alertas)}`,
  });

  // 5. DLQ com motivo no header
  await narrar(page, '5. Leituras inválidas vão para a DLQ', {
    descricao: 'Umidade de 140,5% (invalid_reading) e a mensagem que não é Avro (malformed) vão para greenhouse.readings.dlq com o motivo no header dlq.reason. Erro transitório do registry nunca vai para a DLQ.',
  });
  await abrirTopico(page, 'greenhouse.readings.dlq', DEMO);
  const linhaDlq = page.locator('table tbody tr', { hasText: DEMO }).first();
  await linhaDlq.click();
  const abaHeaders = page.getByText('Headers', { exact: true }).first();
  await abaHeaders.click();
  await expect(page.getByText('dlq.reason').first()).toBeVisible();
  await page.getByText('dlq.reason').first().highlight();
  await page.waitForTimeout(3500);

  // 6. Deduplicação
  const temp = demoRows.find((r) => r.sensor_type === 'TEMPERATURE');
  expect(temp, JSON.stringify(demoRows)).toBeTruthy();
  expect(Number(temp!.sample_count)).toBe(2);
  expect(Number(temp!.media)).toBeCloseTo(45, 2);
  expect(demoRows.find((r) => r.sensor_type === 'HUMIDITY')).toBeUndefined();
  const linhasMetricas = await metricas(request);
  const duplicadas = soma(linhasMetricas, 'telemetry_aggregator_records_total{outcome="duplicate"}');
  expect(duplicadas).toBeGreaterThan(0);
  await narrar(page, '6. A retransmissão foi descartada e não alterou o agregado', {
    descricao: `3 leituras de temperatura publicadas, 2 contadas: sample_count = 2 e média = 45,00 (com a duplicata seria 45,27).
A umidade inválida não gerou agregado. Contador de duplicatas do agregador: ${duplicadas}.`,
    saida: `SELECT ... FROM greenhouse_window_aggregates WHERE greenhouse_id = '${DEMO}';\n\n${tabela(demoRows)}`,
  }, 4500);

  // Redis: último estado por estufa
  const stateUrl = `${AGGREGATOR}/greenhouses/${DEMO}/state`;
  let stateResp = await request.get(stateUrl);
  await expect.poll(async () => (stateResp = await request.get(stateUrl)).status(), { timeout: 30_000 }).toBe(200);
  const state = await stateResp.json();
  expect(state.sensors.TEMPERATURE.sample_count).toBe(2);
  expect(state.sensors.TEMPERATURE.mean).toBeCloseTo(45, 6);
  expect(state.sensors.TEMPERATURE.alert.rule).toBe('ABOVE_MAX');
  await narrar(page, 'Último estado da estufa no Redis', {
    descricao: `O agregador grava o agregado mais recente de cada grandeza num hash por estufa (só sobrescreve com janela mais nova). Leitura em O(1), servida do Redis (header X-State-Source: ${stateResp.headers()['x-state-source']}, ${stateResp.headers()['x-state-read-duration']}).`,
    chamada: { metodo: 'GET', url: stateUrl, status: stateResp.status(), corpo: state },
  }, 4500);
  const hgetall = compose('exec', '-T', 'redis', 'redis-cli', 'HGETALL', `greenhouse:state:${DEMO}`);
  expect(hgetall).toContain('TEMPERATURE');
  const missing = await request.get(`${AGGREGATOR}/greenhouses/gh-inexistente/state`);
  expect(missing.status()).toBe(404);
  await narrar(page, 'A chave no Redis (redis-cli)', {
    descricao: `Estufa sem estado responde 404 (GET /greenhouses/gh-inexistente/state → ${missing.status()}).`,
    saida: `$ redis-cli HGETALL greenhouse:state:${DEMO}\n${hgetall}`,
  }, 4000);

  // 7. Mudança de schema incompatível
  const incompat = readFileSync(path.join(ROOT, 'schemas/testdata/sensor_reading.add-required-field.avsc'), 'utf8');
  const compatUrl = `${REGISTRY}/compatibility/subjects/greenhouse.readings-value/versions/latest?verbose=true`;
  const compat = await request.post(compatUrl, {
    headers: { 'Content-Type': 'application/vnd.schemaregistry.v1+json' }, data: { schema: incompat },
  });
  const compatBody = await compat.json();
  expect(compatBody.is_compatible).toBe(false);
  const registerUrl = `${REGISTRY}/subjects/greenhouse.readings-value/versions`;
  const reg = await request.post(registerUrl, {
    headers: { 'Content-Type': 'application/vnd.schemaregistry.v1+json' }, data: { schema: incompat },
  });
  expect(reg.status()).toBe(409);
  await narrar(page, '7. Schema incompatível é recusado pelo registry', {
    descricao: 'Novo campo obrigatório sem default (schemas/testdata/sensor_reading.add-required-field.avsc): consumidores com o schema novo não leriam as mensagens antigas.',
    chamada: { metodo: 'POST', url: compatUrl.replace(REGISTRY, ''), status: compat.status(), corpo: compatBody },
  }, 4000);
  await narrar(page, '7. Tentativa de registrar mesmo assim', {
    descricao: 'O registry recusa com 409: a compatibilidade BACKWARD do subject é aplicada no servidor, não só no CI.',
    chamada: { metodo: 'POST', url: registerUrl.replace(REGISTRY, ''), status: reg.status(), corpo: await reg.json() },
  }, 4000);

  // 8. Métricas
  const finais = await metricas(request);
  const consumidos = soma(finais, 'telemetry_aggregator_records_consumed_total');
  expect(consumidos).toBeGreaterThan(0);
  const interessantes = finais.filter((l) => /consumer_lag|records_consumed_total|aggregates_emitted_total|alerts_emitted_total|dead_letters_total|state_updates_total|records_total/.test(l));
  await narrar(page, '8. Métricas de lag e throughput em /metrics', {
    descricao: 'Lag por partição, registros consumidos (throughput), janelas, alertas, DLQ e gravações no Redis.',
    chamada: { metodo: 'GET', url: `${AGGREGATOR}/metrics`, status: 200, corpo: interessantes.slice(0, 22).join('\n') },
  }, 4500);
  const expr = 'sum(rate(telemetry_aggregator_records_consumed_total[1m]))';
  await page.goto(`${PROMETHEUS}/graph?g0.expr=${encodeURIComponent(expr)}&g0.tab=0&g0.range_input=15m`);
  await expect(page.locator('canvas, .graph-chart, svg').first()).toBeVisible({ timeout: 30_000 });
  await page.waitForTimeout(3500);
  const lagExpr = 'max by (partition) (telemetry_aggregator_consumer_lag)';
  await page.goto(`${PROMETHEUS}/graph?g0.expr=${encodeURIComponent(lagExpr)}&g0.tab=0&g0.range_input=15m`);
  await page.waitForTimeout(3500);

  // 9. Parquet no MinIO
  let arquivos = '';
  await expect.poll(() => {
    arquivos = compose('exec', '-T', 'minio', 'sh', '-c',
      'mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && mc ls --recursive local/agrovale-telemetry-raw | tail -8');
    return arquivos;
  }, { timeout: 120_000, intervals: [5000] }).toContain('.parquet');
  await narrar(page, '9. Telemetria bruta em Parquet no MinIO (sink S3)', {
    descricao: 'O Kafka Connect arquiva greenhouse.readings em Parquet (snappy), particionado por hora do tempo de evento.',
    saida: `$ mc ls --recursive local/agrovale-telemetry-raw | tail -8\n${arquivos}`,
  }, 4000);
  await page.goto(`${MINIO_CONSOLE}/login`);
  await page.locator('#accessKey').fill('dev-only-minio');
  await page.locator('#secretKey').fill('dev-only-change-me');
  await page.locator('button[type=submit]').click();
  await page.goto(`${MINIO_CONSOLE}/browser/agrovale-telemetry-raw/raw/greenhouse.readings/`);
  await page.waitForTimeout(3000);
  const hora = arquivos.trim().split('\n').pop()!.match(/raw\/greenhouse\.readings\/.*\//)![0];
  await page.goto(`${MINIO_CONSOLE}/browser/agrovale-telemetry-raw/${hora}`);
  await expect(page.getByText('.parquet').first()).toBeVisible({ timeout: 20_000 });
  await page.getByText('.parquet').last().highlight();
  await page.waitForTimeout(3500);

  await narrar(page, 'Resumo: o que foi provado', {
    descricao: `✔ telemetryctl criou tópicos e schemas; subjects com compatibilidade BACKWARD
✔ leituras de várias estufas particionadas por greenhouse_id
✔ janelas de 1 minuto no tópico e no Postgres (sink JDBC, upsert)
✔ alerta ABOVE_MAX da ${DEMO} no tópico e no Postgres
✔ inválida e não-Avro na DLQ com dlq.reason no header
✔ retransmissão deduplicada: 2 amostras, média 45,00
✔ último estado da estufa no Redis, lido por GET /greenhouses/{id}/state
✔ schema incompatível recusado (is_compatible=false, 409)
✔ lag e throughput em /metrics e no Prometheus; Parquet no MinIO`,
  }, 5000);
});
