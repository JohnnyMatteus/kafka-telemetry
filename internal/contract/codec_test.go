package contract_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/contract"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/telemetry"
	"github.com/JohnnyMatteus/kafka-telemetry/schemas"
)

type fakeRegistry struct {
	schemas map[int]string
	calls   int
	err     error
}

func (f *fakeRegistry) SchemaByID(_ context.Context, id int) (string, error) {
	f.calls++
	if f.err != nil {
		return "", f.err
	}
	s, ok := f.schemas[id]
	if !ok {
		return "", fmt.Errorf("id %d: %w", id, contract.ErrUnknownSchema)
	}
	return s, nil
}

func readingVersions(t *testing.T) (v1, v2 string) {
	t.Helper()
	sub, err := schemas.Lookup(schemas.TopicReadings)
	if err != nil {
		t.Fatal(err)
	}
	texts, err := sub.Texts()
	if err != nil {
		t.Fatal(err)
	}
	return texts[0], texts[1]
}

var measured = time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)

func sampleReading() telemetry.Reading {
	return telemetry.Reading{
		EventID:      "6f1c1d3e-8a51-4b0e-9a55-0b5f3c2d9e10",
		GreenhouseID: "gh-003",
		SensorID:     "gh-003-co2-1",
		Type:         telemetry.SensorCO2,
		Value:        812.5,
		MeasuredAt:   measured,
	}
}

func TestWireFrame(t *testing.T) {
	b := contract.AppendFrame(nil, 258, []byte{0xAA})
	if want := []byte{0, 0, 0, 1, 2, 0xAA}; string(b) != string(want) {
		t.Fatalf("frame = %v, quero %v", b, want)
	}
	id, payload, err := contract.ParseFrame(b)
	if err != nil || id != 258 || len(payload) != 1 {
		t.Fatalf("parse = %d %v %v", id, payload, err)
	}
	for name, bad := range map[string][]byte{
		"vazio":          nil,
		"curto":          {0, 0, 1},
		"magic inválido": {1, 0, 0, 0, 1, 0},
		"json cru":       []byte(`{"value": 1}`),
	} {
		if _, _, err := contract.ParseFrame(bad); !errors.Is(err, contract.ErrMalformed) {
			t.Errorf("%s: esperava ErrMalformed, veio %v", name, err)
		}
	}
}

func TestDecodeReadsOlderWriterVersion(t *testing.T) {
	v1, v2 := readingVersions(t)
	reg := &fakeRegistry{schemas: map[int]string{1: v1, 2: v2}}
	encV1, err := contract.NewEncoder(1, v1)
	if err != nil {
		t.Fatal(err)
	}
	msg, err := encV1.Encode(contract.NewSensorReadingRecord(sampleReading(), "fw-ignored-by-v1"))
	if err != nil {
		t.Fatal(err)
	}

	var got contract.SensorReadingRecord
	if err := contract.NewDecoder(reg).Decode(context.Background(), msg, &got); err != nil {
		t.Fatal(err)
	}
	if got.Reading() != sampleReading() {
		t.Fatalf("leitura = %+v", got.Reading())
	}
	if got.FirmwareVersion != nil {
		t.Fatalf("campo opcional de v2 deveria ficar nulo em mensagem v1, veio %q", *got.FirmwareVersion)
	}
}

func TestDecodeLatestVersionAndCacheSchema(t *testing.T) {
	_, v2 := readingVersions(t)
	reg := &fakeRegistry{schemas: map[int]string{7: v2}}
	enc, err := contract.NewEncoder(7, v2)
	if err != nil {
		t.Fatal(err)
	}
	msg, err := enc.Encode(contract.NewSensorReadingRecord(sampleReading(), "2.4.1"))
	if err != nil {
		t.Fatal(err)
	}
	dec := contract.NewDecoder(reg)
	for range 3 {
		var got contract.SensorReadingRecord
		if err := dec.Decode(context.Background(), msg, &got); err != nil {
			t.Fatal(err)
		}
		if got.FirmwareVersion == nil || *got.FirmwareVersion != "2.4.1" || got.Unit != "ppm" {
			t.Fatalf("registro = %+v", got)
		}
	}
	if reg.calls != 1 {
		t.Fatalf("registry chamado %d vezes, o schema deveria vir do cache", reg.calls)
	}
}

func TestDecodeErrorsAreClassified(t *testing.T) {
	_, v2 := readingVersions(t)
	enc, _ := contract.NewEncoder(2, v2)
	valid, _ := enc.Encode(contract.NewSensorReadingRecord(sampleReading(), ""))

	t.Run("schema desconhecido é definitivo", func(t *testing.T) {
		err := contract.NewDecoder(&fakeRegistry{}).Decode(context.Background(), valid, &contract.SensorReadingRecord{})
		if !errors.Is(err, contract.ErrUnknownSchema) {
			t.Fatalf("esperava ErrUnknownSchema, veio %v", err)
		}
	})
	t.Run("registry fora do ar é transitório", func(t *testing.T) {
		outage := errors.New("connection refused")
		err := contract.NewDecoder(&fakeRegistry{err: outage}).Decode(context.Background(), valid, &contract.SensorReadingRecord{})
		if !errors.Is(err, outage) || errors.Is(err, contract.ErrMalformed) || errors.Is(err, contract.ErrUnknownSchema) {
			t.Fatalf("erro deveria ser transitório, veio %v", err)
		}
	})
	t.Run("payload truncado é malformado", func(t *testing.T) {
		reg := &fakeRegistry{schemas: map[int]string{2: v2}}
		err := contract.NewDecoder(reg).Decode(context.Background(), valid[:8], &contract.SensorReadingRecord{})
		if !errors.Is(err, contract.ErrMalformed) {
			t.Fatalf("esperava ErrMalformed, veio %v", err)
		}
	})
}

func TestEncodeRejectsValueOutsideEnum(t *testing.T) {
	_, v2 := readingVersions(t)
	enc, _ := contract.NewEncoder(2, v2)
	r := sampleReading()
	r.Type = "PH"
	if _, err := enc.Encode(contract.NewSensorReadingRecord(r, "")); err == nil {
		t.Fatal("o contrato não deveria aceitar um tipo fora do enum")
	}
}

func TestNewEncoderValidatesInput(t *testing.T) {
	if _, err := contract.NewEncoder(-1, `"string"`); err == nil {
		t.Error("id negativo deveria falhar")
	}
	if _, err := contract.NewEncoder(1, `{"type": "nope"}`); err == nil {
		t.Error("schema inválido deveria falhar")
	}
}
