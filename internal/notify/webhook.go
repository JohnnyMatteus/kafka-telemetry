package notify

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// Cabeçalhos do webhook. A assinatura cobre "<timestamp>.<corpo>", então um
// corpo reaproveitado fora da tolerância de tempo é recusado pelo receptor.
const (
	HeaderTimestamp = "X-Telemetry-Timestamp"
	HeaderSignature = "X-Telemetry-Signature"
	HeaderEventID   = "X-Telemetry-Event-Id"
	signaturePrefix = "v1="
)

// MinSecretLen evita um segredo de exemplo curto virar credencial.
const MinSecretLen = 16

// WebhookChannel envia o alerta em JSON assinado com HMAC-SHA256.
type WebhookChannel struct {
	Client *http.Client
	// AllowHTTP aceita http:// (só para desenvolvimento local).
	AllowHTTP bool
	Now       func() time.Time
}

// NewWebhookChannel devolve um canal cujo cliente não segue redirecionamento.
// O segredo de cada produtor vem do próprio Recipient.
func NewWebhookChannel(allowHTTP bool) *WebhookChannel {
	return &WebhookChannel{
		AllowHTTP: allowHTTP, Now: time.Now,
		Client: &http.Client{
			Timeout: 10 * time.Second,
			// Um 3xx não é seguido: o destino cadastrado é o único que recebe o alerta.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
}

func (c *WebhookChannel) Name() string { return "webhook" }

func (c *WebhookChannel) Address(r Recipient) string { return r.WebhookURL }

type webhookPayload struct {
	AlertID       string    `json:"alert_id"`
	GreenhouseID  string    `json:"greenhouse_id"`
	SensorType    string    `json:"sensor_type"`
	Rule          string    `json:"rule"`
	ObservedValue float64   `json:"observed_value"`
	LimitValue    float64   `json:"limit_value"`
	Unit          string    `json:"unit"`
	WindowStart   time.Time `json:"window_start"`
	WindowEnd     time.Time `json:"window_end"`
	Message       string    `json:"message"`
}

// Sign devolve a assinatura de um corpo.
func Sign(secret []byte, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(timestamp))
	mac.Write([]byte("."))
	mac.Write(body)
	return signaturePrefix + hex.EncodeToString(mac.Sum(nil))
}

// Verify é o que o receptor faz: confere a assinatura em tempo constante e
// recusa timestamps fora da tolerância.
func Verify(secret []byte, timestamp, signature string, body []byte, now time.Time, tolerance time.Duration) error {
	sec, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return errors.New("timestamp inválido")
	}
	if d := now.Sub(time.Unix(sec, 0)); d > tolerance || d < -tolerance {
		return errors.New("timestamp fora da tolerância")
	}
	if subtle.ConstantTimeCompare([]byte(Sign(secret, timestamp, body)), []byte(signature)) != 1 {
		return errors.New("assinatura inválida")
	}
	return nil
}

// Send faz o POST, assinado com o segredo do destinatário. 2xx é sucesso; 408, 429 e 5xx são tentados de novo;
// qualquer outro status é permanente.
func (c *WebhookChannel) Send(ctx context.Context, r Recipient, m Message) error {
	to := r.WebhookURL
	if len(r.WebhookSecret) < MinSecretLen {
		return fmt.Errorf("%w: produtor sem segredo de webhook válido", ErrPermanent)
	}
	u, err := url.Parse(to)
	if err != nil || u.Host == "" || (u.Scheme != "https" && (!c.AllowHTTP || u.Scheme != "http")) {
		return fmt.Errorf("%w: URL do webhook deve ser https", ErrPermanent)
	}
	a := m.Alert
	body, err := json.Marshal(webhookPayload{
		AlertID: a.ID, GreenhouseID: a.GreenhouseID, SensorType: string(a.SensorType), Rule: string(a.Rule),
		ObservedValue: a.Observed, LimitValue: a.Limit, Unit: a.SensorType.Unit(),
		WindowStart: a.WindowStart.UTC(), WindowEnd: a.WindowEnd.UTC(), Message: m.Subject,
	})
	if err != nil {
		return err
	}
	ts := strconv.FormatInt(c.Now().Unix(), 10)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("%w: %w", ErrPermanent, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(HeaderTimestamp, ts)
	req.Header.Set(HeaderSignature, Sign([]byte(r.WebhookSecret), ts, body))
	// O receptor pode deduplicar por este id se um aviso chegar duas vezes.
	req.Header.Set(HeaderEventID, a.ID)
	resp, err := c.Client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return nil
	case resp.StatusCode == http.StatusRequestTimeout, resp.StatusCode == http.StatusTooManyRequests, resp.StatusCode >= 500:
		return fmt.Errorf("webhook respondeu %d", resp.StatusCode)
	default:
		return fmt.Errorf("%w: webhook respondeu %d", ErrPermanent, resp.StatusCode)
	}
}
