package notify_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/aggregation"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/contract"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/notify"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/telemetry"
	"github.com/JohnnyMatteus/kafka-telemetry/schemas"
)

var t0 = time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)

func alert(id string) telemetry.Alert {
	return telemetry.Alert{AlertID: id, GreenhouseID: "gh-001", Type: telemetry.SensorTemperature, Rule: telemetry.RuleAboveMax,
		WindowStart: t0, WindowEnd: t0.Add(time.Minute), Observed: 36.2, Limit: 32}
}

type dir []notify.Recipient

func (d dir) Lookup(context.Context, string) ([]notify.Recipient, error) { return d, nil }

// fakeChannel falha conforme o roteiro e guarda o que entregou.
type fakeChannel struct {
	mu     sync.Mutex
	name   string
	errs   []error
	sent   []string
	calls  int
	useURL bool
}

func (f *fakeChannel) Name() string { return f.name }
func (f *fakeChannel) Address(r notify.Recipient) string {
	if f.useURL {
		return r.WebhookURL
	}
	return r.Email
}
func (f *fakeChannel) Send(_ context.Context, to string, m notify.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if len(f.errs) > 0 {
		err := f.errs[0]
		f.errs = f.errs[1:]
		if err != nil {
			return err
		}
	}
	f.sent = append(f.sent, to+"|"+m.Alert.ID)
	return nil
}

type seen struct {
	mu  sync.Mutex
	got map[string]int
}

func (s *seen) Notified(ch, outcome string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.got == nil {
		s.got = map[string]int{}
	}
	s.got[ch+"/"+outcome]++
}

func newRedisSent(t *testing.T) *notify.RedisSent {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return notify.NewRedisSent(rdb)
}

func newService(t *testing.T, sent notify.Sent, obs notify.Observer, chans ...notify.Channel) *notify.Service {
	t.Helper()
	svc, err := notify.New(notify.Deps{
		Channels: chans, Directory: dir{{ID: "p1", Email: "ana@example.com", WebhookURL: "https://hook.example.com/a"}},
		Sent: sent, Observer: obs, Retry: notify.Retry{Attempts: 3},
		Wait: func(context.Context, time.Duration) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

func domainAlert(id string) notify.Alert {
	return notify.Alert{ID: id, GreenhouseID: "gh-001", SensorType: telemetry.SensorTemperature, Rule: telemetry.RuleAboveMax,
		WindowStart: t0, WindowEnd: t0.Add(time.Minute), Observed: 36.2, Limit: 32}
}

func TestRetriesTransientFailuresThenSendsOnce(t *testing.T) {
	ch := &fakeChannel{name: "email", errs: []error{errors.New("smtp fora"), errors.New("smtp fora")}}
	obs := &seen{}
	svc := newService(t, newRedisSent(t), obs, ch)
	for range 2 { // o mesmo alerta chega duas vezes (restart do agregador)
		if err := svc.Notify(context.Background(), domainAlert("a1")); err != nil {
			t.Fatal(err)
		}
	}
	if ch.calls != 3 || len(ch.sent) != 1 || obs.got["email/sent"] != 1 || obs.got["email/duplicate"] != 1 {
		t.Fatalf("calls=%d sent=%v obs=%v", ch.calls, ch.sent, obs.got)
	}
}

func TestPermanentFailureIsNotRetried(t *testing.T) {
	ch := &fakeChannel{name: "email", errs: []error{fmt.Errorf("%w: 550", notify.ErrPermanent)}}
	obs := &seen{}
	svc := newService(t, newRedisSent(t), obs, ch)
	if err := svc.Notify(context.Background(), domainAlert("a1")); err != nil {
		t.Fatal(err)
	}
	if ch.calls != 1 || obs.got["email/permanent_failure"] != 1 {
		t.Fatalf("calls=%d obs=%v", ch.calls, obs.got)
	}
}

func TestExhaustedRetriesAreNotMarkedSoARedeliveryTriesAgain(t *testing.T) {
	boom := errors.New("fora")
	ch := &fakeChannel{name: "email", errs: []error{boom, boom, boom}}
	obs := &seen{}
	svc := newService(t, newRedisSent(t), obs, ch)
	if err := svc.Notify(context.Background(), domainAlert("a1")); err != nil {
		t.Fatal(err)
	}
	if obs.got["email/failed"] != 1 || len(ch.sent) != 0 {
		t.Fatalf("obs=%v sent=%v", obs.got, ch.sent)
	}
	if err := svc.Notify(context.Background(), domainAlert("a1")); err != nil || len(ch.sent) != 1 {
		t.Fatalf("reentrega: err=%v sent=%v", err, ch.sent)
	}
}

func TestChannelsAreIndependentAndSkipRecipientsWithoutThem(t *testing.T) {
	email, hook := &fakeChannel{name: "email"}, &fakeChannel{name: "webhook", useURL: true, errs: []error{fmt.Errorf("%w: 400", notify.ErrPermanent)}}
	svc, _ := notify.New(notify.Deps{Channels: []notify.Channel{email, hook}, Sent: newRedisSent(t),
		Directory: dir{{ID: "p1", Email: "ana@example.com", WebhookURL: "https://h.example.com"}, {ID: "p2", Email: "bia@example.com"}}})
	if err := svc.Notify(context.Background(), domainAlert("a1")); err != nil {
		t.Fatal(err)
	}
	if len(email.sent) != 2 || hook.calls != 1 {
		t.Fatalf("email=%v webhook calls=%d", email.sent, hook.calls)
	}
}

type brokenSent struct{}

func (brokenSent) Has(context.Context, string) (bool, error) { return false, errors.New("redis fora") }
func (brokenSent) Mark(context.Context, string) error        { return nil }

func TestInfrastructureErrorSurfaces(t *testing.T) {
	svc := newService(t, brokenSent{}, nil, &fakeChannel{name: "email"})
	if err := svc.Notify(context.Background(), domainAlert("a1")); err == nil {
		t.Fatal("o erro do Redis precisa voltar para o offset não ser commitado")
	}
}

func TestNoRecipientIsCountedNotFailed(t *testing.T) {
	obs := &seen{}
	svc, _ := notify.New(notify.Deps{Channels: []notify.Channel{&fakeChannel{name: "email"}}, Sent: newRedisSent(t), Directory: dir{}, Observer: obs})
	if err := svc.Notify(context.Background(), domainAlert("a1")); err != nil || obs.got["none/no_recipient"] != 1 {
		t.Fatalf("err=%v obs=%v", err, obs.got)
	}
}

// --- consumidor: ponto de entrada do tópico de alertas ---

type alertRegistry struct{ text string }

func (r alertRegistry) SchemaByID(context.Context, int) (string, error) { return r.text, nil }

type commits struct {
	got map[aggregation.TopicPartition]int64
}

func (c *commits) Commit(_ context.Context, o map[aggregation.TopicPartition]int64) error {
	for k, v := range o {
		c.got[k] = v
	}
	return nil
}

func encodedAlert(t *testing.T, text string, a telemetry.Alert) []byte {
	t.Helper()
	enc, err := contract.NewEncoder(7, text)
	if err != nil {
		t.Fatal(err)
	}
	v, err := enc.Encode(contract.NewThresholdAlertRecord(a))
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestConsumerNotifiesCommitsAndSkipsUnreadableAlerts(t *testing.T) {
	sub, _ := schemas.Lookup(schemas.TopicAlerts)
	text, _ := sub.Latest()
	ch := &fakeChannel{name: "email"}
	svc := newService(t, newRedisSent(t), nil, ch)
	cm := &commits{got: map[aggregation.TopicPartition]int64{}}
	c := &notify.Consumer{Decoder: contract.NewDecoder(alertRegistry{text}), Service: svc, Committer: cm, Observer: &seen{}}
	tp := aggregation.TopicPartition{Topic: schemas.TopicAlerts, Partition: 2}
	good := encodedAlert(t, text, alert("a1"))
	batch := []aggregation.Record{
		{TopicPartition: tp, Offset: 10, Value: good},
		{TopicPartition: tp, Offset: 11, Value: []byte("lixo")},
		{TopicPartition: tp, Offset: 12, Value: good}, // reentrega do mesmo alerta
	}
	if err := c.HandleBatch(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if len(ch.sent) != 1 || !strings.HasSuffix(ch.sent[0], "|a1") || cm.got[tp] != 13 {
		t.Fatalf("sent=%v commits=%v", ch.sent, cm.got)
	}
}

func TestConsumerDoesNotCommitWhenNotifyFails(t *testing.T) {
	sub, _ := schemas.Lookup(schemas.TopicAlerts)
	text, _ := sub.Latest()
	svc := newService(t, brokenSent{}, nil, &fakeChannel{name: "email"})
	cm := &commits{got: map[aggregation.TopicPartition]int64{}}
	c := &notify.Consumer{Decoder: contract.NewDecoder(alertRegistry{text}), Service: svc, Committer: cm, Observer: &seen{}}
	tp := aggregation.TopicPartition{Topic: schemas.TopicAlerts}
	if err := c.HandleBatch(context.Background(), []aggregation.Record{{TopicPartition: tp, Value: encodedAlert(t, text, alert("a1"))}}); err == nil {
		t.Fatal("esperava erro")
	}
	if len(cm.got) != 0 {
		t.Fatalf("commitou com o aviso pendente: %v", cm.got)
	}
}

// --- adaptador de webhook contra um receptor local ---

func TestWebhookSignsRetriesAndRejectsUnsafeTargets(t *testing.T) {
	secret := "dev-only-webhook-secret"
	var mu sync.Mutex
	var statuses = []int{500, 200}
	var verified int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, 4096)
		n, _ := r.Body.Read(body)
		if err := notify.Verify([]byte(secret), r.Header.Get(notify.HeaderTimestamp), r.Header.Get(notify.HeaderSignature), body[:n], time.Now(), time.Minute); err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		verified++
		code := statuses[0]
		if len(statuses) > 1 {
			statuses = statuses[1:]
		}
		w.WriteHeader(code)
	}))
	defer srv.Close()

	wh, err := notify.NewWebhookChannel(secret, true)
	if err != nil {
		t.Fatal(err)
	}
	svc, _ := notify.New(notify.Deps{Channels: []notify.Channel{wh}, Sent: newRedisSent(t),
		Directory: dir{{ID: "p1", WebhookURL: srv.URL}}, Wait: func(context.Context, time.Duration) error { return nil }})
	if err := svc.Notify(context.Background(), domainAlert("a1")); err != nil {
		t.Fatal(err)
	}
	if verified != 2 {
		t.Fatalf("o receptor viu %d requisições assinadas, quero 2 (500 e depois 200)", verified)
	}

	strict, _ := notify.NewWebhookChannel(secret, false)
	if err := strict.Send(context.Background(), srv.URL, notify.Compose(domainAlert("a2"))); !errors.Is(err, notify.ErrPermanent) {
		t.Fatalf("http sem AllowHTTP deveria ser recusado: %v", err)
	}
	redirect := httptest.NewServer(http.RedirectHandler("http://169.254.169.254/", http.StatusFound))
	defer redirect.Close()
	if err := wh.Send(context.Background(), redirect.URL, notify.Compose(domainAlert("a3"))); !errors.Is(err, notify.ErrPermanent) {
		t.Fatalf("redirecionamento não pode ser seguido: %v", err)
	}
	if _, err := notify.NewWebhookChannel("curto", false); err == nil {
		t.Fatal("segredo curto deveria ser recusado")
	}
}

func TestVerifyRejectsTamperedAndStale(t *testing.T) {
	secret, body := []byte("dev-only-webhook-secret"), []byte(`{"a":1}`)
	now := time.Unix(1_800_000_000, 0)
	ts := "1800000000"
	sig := notify.Sign(secret, ts, body)
	if err := notify.Verify(secret, ts, sig, body, now, time.Minute); err != nil {
		t.Fatal(err)
	}
	if notify.Verify(secret, ts, sig, []byte(`{"a":2}`), now, time.Minute) == nil {
		t.Error("corpo alterado aceito")
	}
	if notify.Verify(secret, ts, sig, body, now.Add(time.Hour), time.Minute) == nil {
		t.Error("timestamp antigo aceito")
	}
}

// --- adaptador de e-mail contra um servidor SMTP local ---

// smtpStub é um servidor SMTP mínimo: aceita um envio e guarda os dados.
func smtpStub(t *testing.T, rcptReply string) (addr string, got chan string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	got = make(chan string, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		rd, w := bufio.NewReader(conn), func(s string) { _, _ = fmt.Fprintf(conn, "%s\r\n", s) }
		w("220 stub ESMTP")
		var data strings.Builder
		inData := false
		for {
			line, err := rd.ReadString('\n')
			if err != nil {
				return
			}
			line = strings.TrimRight(line, "\r\n")
			switch {
			case inData && line == ".":
				inData = false
				got <- data.String()
				w("250 ok")
			case inData:
				data.WriteString(line + "\n")
			case strings.HasPrefix(line, "EHLO"), strings.HasPrefix(line, "HELO"):
				w("250 stub")
			case strings.HasPrefix(line, "MAIL"):
				w("250 ok")
			case strings.HasPrefix(line, "RCPT"):
				w(rcptReply)
			case line == "DATA":
				inData = true
				w("354 go")
			case line == "QUIT":
				w("221 bye")
				return
			}
		}
	}()
	return ln.Addr().String(), got
}

func TestEmailDeliversToSMTPAndClassifiesRefusals(t *testing.T) {
	addr, got := smtpStub(t, "250 ok")
	ch := &notify.EmailChannel{Addr: addr, From: "alertas@example.com"}
	if err := ch.Send(context.Background(), "ana@example.com", notify.Compose(domainAlert("a1"))); err != nil {
		t.Fatal(err)
	}
	data := <-got
	if !strings.Contains(data, "To: <ana@example.com>") || !strings.Contains(data, "X-Alert-ID: a1") || !strings.Contains(data, "temperatura") {
		t.Fatalf("mensagem: %q", data)
	}

	addr, _ = smtpStub(t, "550 no such user")
	ch = &notify.EmailChannel{Addr: addr, From: "alertas@example.com"}
	if err := ch.Send(context.Background(), "x@example.com", notify.Compose(domainAlert("a1"))); !errors.Is(err, notify.ErrPermanent) {
		t.Fatalf("5xx deveria ser permanente: %v", err)
	}
	addr, _ = smtpStub(t, "451 try later")
	ch = &notify.EmailChannel{Addr: addr, From: "alertas@example.com"}
	if err := ch.Send(context.Background(), "x@example.com", notify.Compose(domainAlert("a1"))); err == nil || errors.Is(err, notify.ErrPermanent) {
		t.Fatalf("4xx deveria ser transitório: %v", err)
	}
	if err := ch.Send(context.Background(), "x@example.com\r\nBcc: y@example.com", notify.Compose(domainAlert("a1"))); !errors.Is(err, notify.ErrPermanent) {
		t.Fatalf("injeção de cabeçalho: %v", err)
	}
}

func TestDirectoryValidation(t *testing.T) {
	ok := `{"producers":[{"id":"p1","name":"Ana","email":"ana@example.com","greenhouses":["gh-001"]}]}`
	d, err := notify.ParseDirectory([]byte(ok))
	if err != nil {
		t.Fatal(err)
	}
	if r, _ := d.Lookup(context.Background(), "gh-001"); len(r) != 1 || r[0].Email != "ana@example.com" {
		t.Fatalf("lookup = %v", r)
	}
	for name, bad := range map[string]string{
		"vazio":              `{"producers":[]}`,
		"sem contato":        `{"producers":[{"id":"p1","greenhouses":["gh-001"]}]}`,
		"sem estufa":         `{"producers":[{"id":"p1","email":"a@example.com"}]}`,
		"id repetido":        `{"producers":[{"id":"p1","email":"a@example.com","greenhouses":["g"]},{"id":"p1","email":"b@example.com","greenhouses":["g"]}]}`,
		"email ruim":         `{"producers":[{"id":"p1","email":"nao-e-email","greenhouses":["g"]}]}`,
		"campo extra":        `{"producers":[{"id":"p1","email":"a@example.com","greenhouses":["g"],"senha":"x"}]}`,
		"id com dois-pontos": `{"producers":[{"id":"p:1","email":"a@example.com","greenhouses":["g"]}]}`,
	} {
		if _, err := notify.ParseDirectory([]byte(bad)); err == nil {
			t.Errorf("%s: esperava erro", name)
		}
	}
}
