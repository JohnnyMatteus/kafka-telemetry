package contract

import (
	"time"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/telemetry"
)

// SensorReadingRecord espelha a versão mais nova de SensorReading
// (schemas/avro/sensor_reading.v2.avsc).
type SensorReadingRecord struct {
	EventID         string    `avro:"event_id"`
	GreenhouseID    string    `avro:"greenhouse_id"`
	SensorID        string    `avro:"sensor_id"`
	SensorType      string    `avro:"sensor_type"`
	Value           float64   `avro:"value"`
	Unit            string    `avro:"unit"`
	MeasuredAt      time.Time `avro:"measured_at"`
	FirmwareVersion *string   `avro:"firmware_version"`
}

// NewSensorReadingRecord converte a leitura do domínio para o contrato.
func NewSensorReadingRecord(r telemetry.Reading, firmware string) SensorReadingRecord {
	rec := SensorReadingRecord{
		EventID:      r.EventID,
		GreenhouseID: r.GreenhouseID,
		SensorID:     r.SensorID,
		SensorType:   string(r.Type),
		Value:        r.Value,
		Unit:         r.Type.Unit(),
		MeasuredAt:   r.MeasuredAt.UTC(),
	}
	if firmware != "" {
		rec.FirmwareVersion = &firmware
	}
	return rec
}

// Reading converte o contrato para o domínio. A validação fica no domínio.
func (r SensorReadingRecord) Reading() telemetry.Reading {
	return telemetry.Reading{
		EventID:      r.EventID,
		GreenhouseID: r.GreenhouseID,
		SensorID:     r.SensorID,
		Type:         telemetry.SensorType(r.SensorType),
		Value:        r.Value,
		MeasuredAt:   r.MeasuredAt,
	}
}

// WindowAggregateRecord espelha schemas/avro/window_aggregate.v1.avsc.
type WindowAggregateRecord struct {
	GreenhouseID string    `avro:"greenhouse_id"`
	SensorType   string    `avro:"sensor_type"`
	WindowStart  time.Time `avro:"window_start"`
	WindowEnd    time.Time `avro:"window_end"`
	SampleCount  int       `avro:"sample_count"`
	MinValue     float64   `avro:"min_value"`
	MaxValue     float64   `avro:"max_value"`
	MeanValue    float64   `avro:"mean_value"`
	Unit         string    `avro:"unit"`
}

// NewWindowAggregateRecord converte o agregado do domínio para o contrato.
func NewWindowAggregateRecord(a telemetry.Aggregate) WindowAggregateRecord {
	return WindowAggregateRecord{
		GreenhouseID: a.Key.GreenhouseID,
		SensorType:   string(a.Key.Type),
		WindowStart:  a.Key.Start.UTC(),
		WindowEnd:    a.End.UTC(),
		SampleCount:  a.Count,
		MinValue:     a.Min,
		MaxValue:     a.Max,
		MeanValue:    a.Mean,
		Unit:         a.Key.Type.Unit(),
	}
}

// ThresholdAlertRecord espelha schemas/avro/threshold_alert.v1.avsc.
type ThresholdAlertRecord struct {
	AlertID       string    `avro:"alert_id"`
	GreenhouseID  string    `avro:"greenhouse_id"`
	SensorType    string    `avro:"sensor_type"`
	WindowStart   time.Time `avro:"window_start"`
	WindowEnd     time.Time `avro:"window_end"`
	Rule          string    `avro:"rule"`
	ObservedValue float64   `avro:"observed_value"`
	LimitValue    float64   `avro:"limit_value"`
}

// NewThresholdAlertRecord converte o alerta do domínio para o contrato.
func NewThresholdAlertRecord(a telemetry.Alert) ThresholdAlertRecord {
	return ThresholdAlertRecord{
		AlertID:       a.AlertID,
		GreenhouseID:  a.GreenhouseID,
		SensorType:    string(a.Type),
		WindowStart:   a.WindowStart.UTC(),
		WindowEnd:     a.WindowEnd.UTC(),
		Rule:          string(a.Rule),
		ObservedValue: a.Observed,
		LimitValue:    a.Limit,
	}
}
