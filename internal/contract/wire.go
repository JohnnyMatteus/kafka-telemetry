// Package contract implementa os contratos de mensagem do pipeline: o
// formato de fio do Schema Registry, a (de)serialização Avro, os registros
// trocados nos tópicos e a checagem de compatibilidade entre versões.
package contract

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// magicByte abre toda mensagem no formato de fio do Confluent Schema Registry:
// [0x00][id do schema: uint32 big-endian][payload Avro].
const magicByte byte = 0

const headerSize = 5

// ErrMalformed indica bytes que não respeitam o contrato. Não adianta tentar
// de novo: a mensagem segue para a DLQ.
var ErrMalformed = errors.New("malformed message")

// AppendFrame escreve o cabeçalho do formato de fio seguido do payload.
func AppendFrame(dst []byte, schemaID uint32, payload []byte) []byte {
	dst = append(dst, magicByte)
	dst = binary.BigEndian.AppendUint32(dst, schemaID)
	return append(dst, payload...)
}

// ParseFrame separa o id do schema e o payload.
func ParseFrame(b []byte) (uint32, []byte, error) {
	if len(b) < headerSize {
		return 0, nil, fmt.Errorf("%w: %d bytes, mínimo %d", ErrMalformed, len(b), headerSize)
	}
	if b[0] != magicByte {
		return 0, nil, fmt.Errorf("%w: magic byte 0x%02x", ErrMalformed, b[0])
	}
	return binary.BigEndian.Uint32(b[1:headerSize]), b[headerSize:], nil
}
