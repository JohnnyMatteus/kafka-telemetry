package notify

import (
	"fmt"
	"strings"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/telemetry"
)

var sensorNames = map[telemetry.SensorType]string{
	telemetry.SensorTemperature: "temperatura",
	telemetry.SensorHumidity:    "umidade",
	telemetry.SensorCO2:         "CO2",
	telemetry.SensorLuminosity:  "luminosidade",
}

// Compose monta o texto do aviso. Só usa valores do alerta, nunca texto livre.
func Compose(a Alert) Message {
	name, ok := sensorNames[a.SensorType]
	if !ok {
		name = strings.ToLower(string(a.SensorType))
	}
	side := "acima do limite máximo"
	if a.Rule == telemetry.RuleBelowMin {
		side = "abaixo do limite mínimo"
	}
	unit := a.SensorType.Unit()
	subject := fmt.Sprintf("Alerta na estufa %s: %s %s", a.GreenhouseID, name, side)
	body := fmt.Sprintf("A média de %s na estufa %s ficou em %.1f %s, %s (%.1f %s).\nJanela: %s a %s.\nAlerta: %s\n",
		name, a.GreenhouseID, a.Observed, unit, side, a.Limit, unit,
		a.WindowStart.UTC().Format("2006-01-02 15:04 UTC"), a.WindowEnd.UTC().Format("15:04 UTC"), a.ID)
	return Message{Alert: a, Subject: subject, Body: body}
}
