package metrics

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStaleBytesTimelyPercentAllOnTime(t *testing.T) {
	agg := NewStaleBytesAgg()
	agg.Add(1000, false)
	agg.Add(500, false)

	require.InDelta(t, 0.0, agg.RatioPercent(), 1e-9)
	require.InDelta(t, 100.0, agg.TimelyPercent(), 1e-9)
}

func TestStaleBytesTimelyPercentAllLate(t *testing.T) {
	agg := NewStaleBytesAgg()
	agg.Add(300, true)
	agg.Add(700, true)

	require.InDelta(t, 100.0, agg.RatioPercent(), 1e-9)
	require.InDelta(t, 0.0, agg.TimelyPercent(), 1e-9)
}

func TestStaleBytesTimelyPercentMixed(t *testing.T) {
	agg := NewStaleBytesAgg()
	agg.Add(200, true)
	agg.Add(800, false)

	require.InDelta(t, 20.0, agg.RatioPercent(), 1e-9)
	require.InDelta(t, 80.0, agg.TimelyPercent(), 1e-9)
}

func TestStaleBytesTimelyPercentZeroBytes(t *testing.T) {
	agg := NewStaleBytesAgg()

	require.InDelta(t, 0.0, agg.RatioPercent(), 1e-9)
	require.InDelta(t, 0.0, agg.TimelyPercent(), 1e-9)
}

func TestStaleBytesTimelyAndStaleAreComplementary(t *testing.T) {
	agg := NewStaleBytesAgg()
	agg.Add(111, true)
	agg.Add(222, false)
	agg.Add(333, true)
	agg.Add(444, false)

	sum := agg.RatioPercent() + agg.TimelyPercent()
	require.InDelta(t, 100.0, sum, 1e-9)
}
