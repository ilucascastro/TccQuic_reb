package session

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"main/src/model"
)

type fakeSizeProvider struct {
	size int64
}

func (f fakeSizeProvider) AvgSize(tileID int) int64 {
	return f.size
}

func TestBolaGuardrailFallsBack(t *testing.T) {
	abr := newBolaFiniteABRWithEstimator(fakeSizeProvider{size: 100})
	ctx := SegmentContext{
		SegmentID:       1,
		FirstSegment:    1,
		LastSegment:     3,
		SegmentDuration: time.Second,
		TimeBudget:      200 * time.Millisecond,
		AvgThroughput:   50,
		BufferLevel:     3 * time.Second,
		FOVTiles:        []int{1, 2},
		AllTiles:        []int{1, 2, 3, 4},
	}

	cfg := abr.SelectConfig(ctx)
	require.Equal(t, "A_all_low", cfg.ID)
	require.Equal(t, model.LOW_BITRATE, cfg.FOVBitrate)
	require.Equal(t, model.LOW_BITRATE, cfg.NonFOVBitrate)
}

func TestBolaNoFOVDefaultsToLow(t *testing.T) {
	abr := newBolaFiniteABRWithEstimator(fakeSizeProvider{size: 100})
	ctx := SegmentContext{
		SegmentID:       1,
		FirstSegment:    1,
		LastSegment:     3,
		SegmentDuration: time.Second,
		TimeBudget:      time.Second,
		AvgThroughput:   1_000_000,
		BufferLevel:     3 * time.Second,
		FOVTiles:        nil,
		AllTiles:        []int{1, 2, 3},
	}

	cfg := abr.SelectConfig(ctx)
	require.Equal(t, "A_all_low", cfg.ID)
	require.Equal(t, model.LOW_BITRATE, cfg.FOVBitrate)
	require.Equal(t, model.LOW_BITRATE, cfg.NonFOVBitrate)
}
