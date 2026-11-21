package state

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/aggregation"
)

// keyPrefix + greenhouse_id é o hash da estufa; cada campo é um tipo de
// sensor com o JSON do último agregado.
const keyPrefix = "greenhouse:state:"

// Só grava se a janela for igual ou mais nova que a guardada: um replay
// depois de crash reemite janelas antigas e não pode regredir o estado.
var upsertNewer = redis.NewScript(`
local cur = redis.call('HGET', KEYS[1], ARGV[1])
if cur then
  local ok, decoded = pcall(cjson.decode, cur)
  if ok and decoded.window_start_ms and tonumber(decoded.window_start_ms) > tonumber(ARGV[2]) then
    return 0
  end
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
return 1
`)

// RedisStore implementa aggregation.StateStore e Reader com um hash por estufa.
type RedisStore struct {
	client redis.UniversalClient
}

// NewRedisStore usa um cliente já configurado.
func NewRedisStore(client redis.UniversalClient) *RedisStore {
	return &RedisStore{client: client}
}

// Key devolve a chave do hash de uma estufa.
func Key(greenhouseID string) string { return keyPrefix + greenhouseID }

type storedSensor struct {
	Sensor
	WindowStartMs int64 `json:"window_start_ms"`
}

// Update grava os estados num pipeline (uma ida ao Redis por lote).
func (s *RedisStore) Update(ctx context.Context, states []aggregation.SensorState) error {
	pipe := s.client.Pipeline()
	for _, st := range states {
		agg := st.Aggregate
		v := storedSensor{
			Sensor: Sensor{
				WindowStart: agg.Key.Start.UTC(), WindowEnd: agg.End.UTC(), SampleCount: agg.Count,
				Mean: agg.Mean, Min: agg.Min, Max: agg.Max, Unit: agg.Key.Type.Unit(),
			},
			WindowStartMs: agg.Key.Start.UnixMilli(),
		}
		if a := st.Alert; a != nil {
			v.Alert = &Alert{Rule: string(a.Rule), Observed: a.Observed, Limit: a.Limit}
		}
		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Errorf("serializar estado: %w", err)
		}
		upsertNewer.Eval(ctx, pipe, []string{Key(agg.Key.GreenhouseID)}, string(agg.Key.Type), v.WindowStartMs, b)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("atualizar estado no redis: %w", err)
	}
	return nil
}

// Get lê o hash da estufa.
func (s *RedisStore) Get(ctx context.Context, greenhouseID string) (Greenhouse, error) {
	fields, err := s.client.HGetAll(ctx, Key(greenhouseID)).Result()
	if err != nil {
		return Greenhouse{}, fmt.Errorf("ler estado no redis: %w", err)
	}
	if len(fields) == 0 {
		return Greenhouse{}, ErrNotFound
	}
	out := Greenhouse{GreenhouseID: greenhouseID, Sensors: make(map[string]Sensor, len(fields))}
	for sensorType, raw := range fields {
		var v storedSensor
		if err := json.Unmarshal([]byte(raw), &v); err != nil {
			return Greenhouse{}, fmt.Errorf("estado corrompido em %s/%s: %w", greenhouseID, sensorType, err)
		}
		out.Sensors[sensorType] = v.Sensor
	}
	return out, nil
}
