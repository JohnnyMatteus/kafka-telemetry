package dlq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	keyDone   = "dlq:replay:done"
	keyCursor = "dlq:replay:cursor"
	keyLock   = "dlq:replay:lock"
)

// Só apaga o lock se ainda for dele: uma execução lenta não libera o lock de
// outra que o adquiriu depois do TTL.
var unlock = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
`)

// RedisLedger implementa Ledger no Redis: um hash offset -> recibo, o cursor
// e o lock da execução.
type RedisLedger struct{ client redis.UniversalClient }

// NewRedisLedger usa um cliente já configurado.
func NewRedisLedger(client redis.UniversalClient) *RedisLedger { return &RedisLedger{client: client} }

func (l *RedisLedger) Lock(ctx context.Context, runID string, ttl time.Duration) (func(), error) {
	ok, err := l.client.SetNX(ctx, keyLock, runID, ttl).Result()
	if err != nil {
		return nil, fmt.Errorf("lock do reprocessamento: %w", err)
	}
	if !ok {
		return nil, ErrBusy
	}
	return func() {
		_ = unlock.Run(context.WithoutCancel(ctx), l.client, []string{keyLock}, runID).Err()
	}, nil
}

func (l *RedisLedger) Done(ctx context.Context, offsets []int64) (map[int64]Receipt, error) {
	out := map[int64]Receipt{}
	if len(offsets) == 0 {
		return out, nil
	}
	fields := make([]string, len(offsets))
	for i, o := range offsets {
		fields[i] = strconv.FormatInt(o, 10)
	}
	vals, err := l.client.HMGet(ctx, keyDone, fields...).Result()
	if err != nil {
		return nil, err
	}
	for i, v := range vals {
		s, ok := v.(string)
		if !ok {
			continue
		}
		var r Receipt
		if err := json.Unmarshal([]byte(s), &r); err != nil {
			return nil, fmt.Errorf("recibo do offset %d: %w", offsets[i], err)
		}
		out[offsets[i]] = r
	}
	return out, nil
}

func (l *RedisLedger) Record(ctx context.Context, receipts map[int64]Receipt) error {
	if len(receipts) == 0 {
		return nil
	}
	values := make(map[string]any, len(receipts))
	for o, r := range receipts {
		b, err := json.Marshal(r)
		if err != nil {
			return err
		}
		values[strconv.FormatInt(o, 10)] = string(b)
	}
	return l.client.HSet(ctx, keyDone, values).Err()
}

func (l *RedisLedger) Cursor(ctx context.Context) (int64, error) {
	n, err := l.client.Get(ctx, keyCursor).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	return n, err
}

func (l *RedisLedger) SetCursor(ctx context.Context, offset int64) error {
	return l.client.Set(ctx, keyCursor, offset, 0).Err()
}
