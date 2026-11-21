// Package schemas versiona os contratos Avro dos tópicos do pipeline.
//
// Cada subject segue a TopicNameStrategy do Schema Registry (<tópico>-value)
// e lista suas versões em ordem. A última versão é a que os produtores usam;
// as anteriores existem para conferir compatibilidade e reproduzir o histórico.
package schemas

import (
	"embed"
	"fmt"
)

//go:embed avro/*.avsc
var files embed.FS

// Nomes dos tópicos do pipeline.
const (
	TopicReadings    = "greenhouse.readings"
	TopicAggregates  = "greenhouse.aggregates"
	TopicAlerts      = "greenhouse.alerts"
	TopicReadingsDLQ = "greenhouse.readings.dlq"
	TopicCheckpoints = "greenhouse.aggregator.checkpoints"
)

// Subject descreve um subject do Schema Registry e suas versões no repositório.
type Subject struct {
	Name     string
	Topic    string
	Versions []string
}

// Subjects é o catálogo de contratos, na ordem em que devem ser registrados.
var Subjects = []Subject{
	{Name: TopicReadings + "-value", Topic: TopicReadings, Versions: []string{"avro/sensor_reading.v1.avsc", "avro/sensor_reading.v2.avsc"}},
	{Name: TopicAggregates + "-value", Topic: TopicAggregates, Versions: []string{"avro/window_aggregate.v1.avsc"}},
	{Name: TopicAlerts + "-value", Topic: TopicAlerts, Versions: []string{"avro/threshold_alert.v1.avsc"}},
}

// Texts devolve o texto de todas as versões do subject, da mais antiga à mais nova.
func (s Subject) Texts() ([]string, error) {
	texts := make([]string, 0, len(s.Versions))
	for _, path := range s.Versions {
		b, err := files.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("schema %s: %w", path, err)
		}
		texts = append(texts, string(b))
	}
	return texts, nil
}

// Latest devolve o texto da versão mais nova do subject.
func (s Subject) Latest() (string, error) {
	texts, err := s.Texts()
	if err != nil {
		return "", err
	}
	return texts[len(texts)-1], nil
}

// Lookup encontra um subject pelo nome do tópico.
func Lookup(topic string) (Subject, error) {
	for _, s := range Subjects {
		if s.Topic == topic {
			return s, nil
		}
	}
	return Subject{}, fmt.Errorf("nenhum schema para o tópico %q", topic)
}
