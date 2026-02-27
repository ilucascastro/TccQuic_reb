package metrics

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDeadlineLatenessAllOnTime(t *testing.T) {
	agg := NewDeadlineLatenessAgg()
	agg.SetRequired(1, []int{100, 101})

	lateness, complete := agg.Record(1, 100, 0)
	require.False(t, complete)
	require.Equal(t, time.Duration(0), lateness)

	lateness, complete = agg.Record(1, 101, 0)
	require.True(t, complete)
	require.Equal(t, time.Duration(0), lateness)

	series := agg.Series(1, 1)
	require.Len(t, series, 1)
	require.Equal(t, 1, series[0].Segment)
	require.Equal(t, 101, series[0].LastTile)
	require.Equal(t, 0.0, series[0].LatenessMs)
	require.Equal(t, 2, series[0].Required)
	require.Equal(t, 2, series[0].Processed)
}

func TestDeadlineLatenessKeepsMaxLateness(t *testing.T) {
	agg := NewDeadlineLatenessAgg()
	agg.SetRequired(7, []int{1, 2, 3})

	agg.Record(7, 1, 5*time.Millisecond)
	agg.Record(7, 2, 0)
	lateness, complete := agg.Record(7, 3, 2*time.Millisecond)

	require.True(t, complete)
	require.Equal(t, 5*time.Millisecond, lateness)

	series := agg.Series(7, 7)
	require.Len(t, series, 1)
	require.Equal(t, 1, series[0].LastTile)
	require.InDelta(t, 5.0, series[0].LatenessMs, 0.0001)
}

func TestDeadlineLatenessTimeoutIsPositive(t *testing.T) {
	agg := NewDeadlineLatenessAgg()
	agg.SetRequired(3, []int{10})

	lateness, complete := agg.Record(3, 10, 11*time.Millisecond)

	require.True(t, complete)
	require.Equal(t, 11*time.Millisecond, lateness)

	series := agg.Series(3, 3)
	require.Len(t, series, 1)
	require.InDelta(t, 11.0, series[0].LatenessMs, 0.0001)
}

func TestDeadlineLatenessIgnoresDuplicateTile(t *testing.T) {
	agg := NewDeadlineLatenessAgg()
	agg.SetRequired(9, []int{1, 2})

	agg.Record(9, 1, 5*time.Millisecond)
	agg.Record(9, 1, 20*time.Millisecond)
	lateness, complete := agg.Record(9, 2, 1*time.Millisecond)

	require.True(t, complete)
	require.Equal(t, 5*time.Millisecond, lateness)

	series := agg.Series(9, 9)
	require.Len(t, series, 1)
	require.Equal(t, 1, series[0].LastTile)
	require.Equal(t, 2, series[0].Processed)
}

func TestDeadlineLatenessSeriesOnlyCompletedAndOrdered(t *testing.T) {
	agg := NewDeadlineLatenessAgg()
	agg.SetRequired(1, []int{10})
	agg.SetRequired(2, []int{20, 21})
	agg.SetRequired(3, []int{})

	agg.Record(2, 20, 3*time.Millisecond)
	agg.Record(1, 10, 0)

	series := agg.Series(1, 3)
	require.Len(t, series, 1)
	require.Equal(t, 1, series[0].Segment)

	agg.Record(2, 21, 1*time.Millisecond)

	series = agg.Series(1, 3)
	require.Len(t, series, 2)
	require.Equal(t, 1, series[0].Segment)
	require.Equal(t, 2, series[1].Segment)
}

func TestDeadlineLatenessTieUsesLastRecordedTile(t *testing.T) {
	agg := NewDeadlineLatenessAgg()
	agg.SetRequired(11, []int{200, 201, 202})

	agg.Record(11, 200, 7*time.Millisecond)
	agg.Record(11, 201, 2*time.Millisecond)
	lateness, complete := agg.Record(11, 202, 7*time.Millisecond)

	require.True(t, complete)
	require.Equal(t, 7*time.Millisecond, lateness)

	series := agg.Series(11, 11)
	require.Len(t, series, 1)
	require.Equal(t, 202, series[0].LastTile)
	require.InDelta(t, 7.0, series[0].LatenessMs, 0.0001)
}
