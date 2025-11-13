package session

import (
	"log"
	"time"

	"main/src/model"
)

// ABRController encapsulates bitrate selection so the session orchestration
// only depends on an interface and different strategies can be plugged in.
type ABRController interface {
	Select(avgThroughput float64, bufferLevel time.Duration) model.Bitrate
}

// BitrateInfo associates a bitrate value with the minimum throughput threshold
// required to safely request it.
type BitrateInfo struct {
	Bitrate   model.Bitrate
	Threshold float64
}

type bufferAwareABR struct {
	bitrates []BitrateInfo
}

// NewDefaultABRController returns the existing heuristic packaged inside the
// ABRController interface.
func NewDefaultABRController() ABRController {
	return &bufferAwareABR{
		bitrates: []BitrateInfo{
			{Bitrate: model.HIGH_BITRATE, Threshold: 60000.0},
			{Bitrate: model.MEDIUM_BITRATE, Threshold: 30000.0},
			{Bitrate: model.LOW_BITRATE, Threshold: 0.0},
		},
	}
}

func (c *bufferAwareABR) Select(avgThroughput float64, bufferLevel time.Duration) model.Bitrate {
	const (
		minBufferLevel = 2 * time.Second
		maxBufferLevel = 10 * time.Second
	)

	if bufferLevel < minBufferLevel {
		log.Printf("ABR (Buffer): Buffer level (%v) is below minimum (%v). Forcing LOW_BITRATE.", bufferLevel, minBufferLevel)
		return model.LOW_BITRATE
	}

	if bufferLevel > maxBufferLevel {
		// Allow aggressive selection when buffer is healthy by checking thresholds
		// from highest to lowest. No extra logic needed—the ordering handles it.
	}

	for _, brInfo := range c.bitrates {
		if avgThroughput >= brInfo.Threshold {
			return brInfo.Bitrate
		}
	}
	return model.LOW_BITRATE
}
