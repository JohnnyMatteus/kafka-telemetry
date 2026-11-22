package notify

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"mime"
	"net"
	"net/mail"
	"net/smtp"
	"net/textproto"
	"strings"
	"time"
)

// EmailChannel envia por SMTP. No compose o destino é o Mailpit.
type EmailChannel struct {
	Addr     string
	From     string
	Username string
	Password string
	Timeout  time.Duration
}

func (c *EmailChannel) Name() string { return "email" }

func (c *EmailChannel) Address(r Recipient) string { return r.Email }

// Send entrega uma mensagem. Resposta 5xx do servidor e endereço inválido são
// permanentes; 4xx e falhas de rede são tentadas de novo.
func (c *EmailChannel) Send(ctx context.Context, r Recipient, m Message) error {
	to := r.Email
	rcpt, err := mail.ParseAddress(to)
	if err != nil || strings.ContainsAny(to, "\r\n") {
		return fmt.Errorf("%w: destinatário inválido", ErrPermanent)
	}
	from, err := mail.ParseAddress(c.From)
	if err != nil {
		return fmt.Errorf("%w: remetente inválido", ErrPermanent)
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "tcp", c.Addr)
	if err != nil {
		return err
	}
	_ = conn.SetDeadline(time.Now().Add(timeout))
	host, _, _ := net.SplitHostPort(c.Addr)
	cl, err := smtp.NewClient(conn, host)
	if err != nil {
		_ = conn.Close()
		return classifySMTP(err)
	}
	defer func() { _ = cl.Close() }()
	if ok, _ := cl.Extension("STARTTLS"); ok {
		if err := cl.StartTLS(&tls.Config{ServerName: host, MinVersion: tls.VersionTLS12}); err != nil {
			return err
		}
	}
	if c.Username != "" {
		// PlainAuth recusa enviar a senha sem TLS, exceto para localhost.
		if err := cl.Auth(smtp.PlainAuth("", c.Username, c.Password, host)); err != nil {
			return classifySMTP(err)
		}
	}
	if err := cl.Mail(from.Address); err != nil {
		return classifySMTP(err)
	}
	if err := cl.Rcpt(rcpt.Address); err != nil {
		return classifySMTP(err)
	}
	w, err := cl.Data()
	if err != nil {
		return classifySMTP(err)
	}
	if _, err := w.Write([]byte(c.render(from, rcpt, m))); err != nil {
		return classifySMTP(err)
	}
	if err := w.Close(); err != nil {
		return classifySMTP(err)
	}
	return classifySMTP(cl.Quit())
}

func (c *EmailChannel) render(from, to *mail.Address, m Message) string {
	headers := []string{
		"From: " + from.String(),
		"To: " + to.String(),
		"Subject: " + mime.QEncoding.Encode("utf-8", sanitizeHeader(m.Subject)),
		"Date: " + time.Now().UTC().Format(time.RFC1123Z),
		// O id do alerta vem do tópico e não entra cru em cabeçalho: o Message-ID
		// é um hash dele.
		"Message-ID: <" + messageIDLocal(m.Alert.ID) + "@telemetry.example.com>",
		"MIME-Version: 1.0",
		"Content-Type: text/plain; charset=utf-8",
		"Content-Transfer-Encoding: 8bit",
		"X-Alert-ID: " + sanitizeHeader(m.Alert.ID),
	}
	return strings.Join(headers, "\r\n") + "\r\n\r\n" + strings.ReplaceAll(m.Body, "\n", "\r\n")
}

func messageIDLocal(alertID string) string {
	sum := sha256.Sum256([]byte(alertID))
	return hex.EncodeToString(sum[:16])
}

func sanitizeHeader(s string) string {
	return strings.NewReplacer("\r", " ", "\n", " ").Replace(s)
}

func classifySMTP(err error) error {
	if err == nil {
		return nil
	}
	var te *textproto.Error
	if errors.As(err, &te) && te.Code >= 500 {
		return fmt.Errorf("%w: %w", ErrPermanent, err)
	}
	return err
}
