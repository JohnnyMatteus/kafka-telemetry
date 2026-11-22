package notify

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// sentTTL é por quanto tempo um aviso enviado impede a repetição; passa com
// folga da retenção do tópico de alertas (30 dias).
const sentTTL = 45 * 24 * time.Hour

// RedisSent implementa Sent com uma chave por aviso.
type RedisSent struct{ client redis.UniversalClient }

// NewRedisSent usa um cliente já configurado.
func NewRedisSent(client redis.UniversalClient) *RedisSent { return &RedisSent{client: client} }

func (r *RedisSent) Has(ctx context.Context, key string) (bool, error) {
	n, err := r.client.Exists(ctx, key).Result()
	return n > 0, err
}

func (r *RedisSent) Mark(ctx context.Context, key string) error {
	return r.client.Set(ctx, key, time.Now().UTC().Format(time.RFC3339), sentTTL).Err()
}
