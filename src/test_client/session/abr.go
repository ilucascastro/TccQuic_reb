package session

import (
	"log"
	"time"

	"main/src/model"
)

// O ABRController encapsula a seleção de bitrate, de modo que a orquestração da sessão
// depende apenas de uma interface e diferentes estratégias podem ser integradas.
type ABRController interface {
	Select(avgThroughput float64, bufferLevel time.Duration, inFOV bool) model.Bitrate
}

// BitrateInfo associa um valor de bitrate (taxa de bits) ao threshold (limite mínimo)
// de throughput (taxa de transferência) necessário para solicitá-lo com segurança.
type BitrateInfo struct {
	Bitrate   model.Bitrate
	Threshold float64
}

type bufferAwareABR struct {
	bitrates []BitrateInfo
}

const (
	minBufferLevel = 2 * time.Second
	maxBufferLevel = 10 * time.Second
)

// NewDefaultABRController retorna a heurística existente empacotada dentro da
// interface ABRController.
func NewDefaultABRController() ABRController {
	return &bufferAwareABR{
		bitrates: []BitrateInfo{
			{Bitrate: model.HIGH_BITRATE, Threshold: 60000.0},
			{Bitrate: model.MEDIUM_BITRATE, Threshold: 30000.0},
			{Bitrate: model.LOW_BITRATE, Threshold: 0.0},
		},
	}
}

func (c *bufferAwareABR) Select(avgThroughput float64, bufferLevel time.Duration, inFOV bool) model.Bitrate {
	if !inFOV {
		return model.LOW_BITRATE
	}

	if bufferLevel < minBufferLevel {
		log.Printf("ABR (Buffer): Buffer level (%v) is below minimum (%v). Forcing LOW_BITRATE.", bufferLevel, minBufferLevel)
		return model.LOW_BITRATE
	}

	if bufferLevel > maxBufferLevel {
		// Permitir seleção agressiva quando o buffer estiver íntegro, verificando os limites
		// do mais alto para o mais baixo. Nenhuma lógica extra é necessária, a ordenação cuida disso.
	}

	for _, brInfo := range c.bitrates {
		if avgThroughput >= brInfo.Threshold {
			return brInfo.Bitrate
		}
	}
	return model.LOW_BITRATE
}
