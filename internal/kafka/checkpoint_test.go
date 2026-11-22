package kafka

import (
	"fmt"
	"testing"
	"time"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/aggregation"
)

func TestCheckpointKeyRoundTrip(t *testing.T) {
	tp := aggregation.TopicPartition{Topic: "greenhouse.readings", Partition: 11}
	wm := time.Date(2026, 3, 10, 12, 1, 5, 0, time.UTC)
	cp, err := decodeCheckpoint([]byte(checkpointKey(tp)), []byte(fmt.Sprintf(`{"offset":42,"watermark_ms":%d}`, wm.UnixMilli())))
	if err != nil {
		t.Fatal(err)
	}
	if cp.TopicPartition != tp || cp.Offset != 42 || !cp.Watermark.Equal(wm) {
		t.Fatalf("checkpoint = %+v", cp)
	}
}

func TestDecodeCheckpointRejectsGarbage(t *testing.T) {
	for _, tc := range []struct{ key, value string }{
		{"sem-particao", `{"offset":1}`},
		{"topic/abc", `{"offset":1}`},
		{"topic/1", `não é json`},
	} {
		if _, err := decodeCheckpoint([]byte(tc.key), []byte(tc.value)); err == nil {
			t.Errorf("%q/%q deveria falhar", tc.key, tc.value)
		}
	}
}

func TestZeroWatermarkStaysZero(t *testing.T) {
	cp, err := decodeCheckpoint([]byte("t/0"), []byte(`{"offset":0,"watermark_ms":0}`))
	if err != nil || !cp.Watermark.IsZero() {
		t.Fatalf("watermark = %v err = %v", cp.Watermark, err)
	}
}

// O checkpoint novo carrega o watermark de cada estufa; o antigo, sem o mapa,
// continua legível.
func TestCheckpointCarriesPerGreenhouseWatermarks(t *testing.T) {
	a := time.Date(2026, 3, 10, 12, 1, 0, 0, time.UTC)
	b := a.Add(time.Minute)
	cp, err := decodeCheckpoint([]byte("t/0"), []byte(fmt.Sprintf(`{"offset":7,"watermark_ms":%d,"watermarks_ms":{"gh-A":%d,"":%d}}`, b.UnixMilli(), b.UnixMilli(), a.UnixMilli())))
	if err != nil {
		t.Fatal(err)
	}
	if !cp.Watermarks["gh-A"].Equal(b) || !cp.Watermarks[""].Equal(a) || !cp.Watermark.Equal(b) {
		t.Fatalf("checkpoint = %+v", cp)
	}
	legacy, err := decodeCheckpoint([]byte("t/0"), []byte(fmt.Sprintf(`{"offset":7,"watermark_ms":%d}`, a.UnixMilli())))
	if err != nil || len(legacy.Watermarks) != 0 || !legacy.Watermark.Equal(a) {
		t.Fatalf("legacy = %+v err=%v", legacy, err)
	}
}
