package telemetry

import "time"

// Deduplicator descarta leituras repetidas pelo id do evento.
//
// Gateways de campo retransmitem leituras quando não recebem confirmação, e o
// consumidor recebe de novo o que já processou depois de um rebalance ou crash.
// A retenção é medida em tempo de evento, não em tempo de parede: assim o
// resultado de um reprocessamento é o mesmo do processamento original.
type Deduplicator struct {
	seen map[string]time.Time
}

// NewDeduplicator cria um deduplicador vazio.
func NewDeduplicator() *Deduplicator {
	return &Deduplicator{seen: make(map[string]time.Time)}
}

// Seen registra o evento e informa se ele já tinha sido visto.
func (d *Deduplicator) Seen(eventID string, measuredAt time.Time) bool {
	if _, ok := d.seen[eventID]; ok {
		return true
	}
	d.seen[eventID] = measuredAt
	return false
}

// EvictBefore esquece eventos medidos antes do limite. Chamado com o
// watermark: um evento mais antigo que ele seria descartado como atrasado de
// qualquer forma, então guardar o id não protege contra nada.
func (d *Deduplicator) EvictBefore(limit time.Time) {
	for id, at := range d.seen {
		if at.Before(limit) {
			delete(d.seen, id)
		}
	}
}

// Len devolve quantos ids estão retidos.
func (d *Deduplicator) Len() int { return len(d.seen) }
