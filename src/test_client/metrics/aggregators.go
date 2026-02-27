package metrics

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"time"
)

// Session aggregates all metric collectors needed by the test client.
type Session struct {
	AllTiles   *SegmentCompletionAgg
	FOVTiles   *SegmentCompletionAgg
	Stale      *StaleBytesAgg
	Deadlines  *TileDeadlineMissAgg
	FOVHit     *FovHitAgg
	FOVGoodput *FovGoodputAgg
	DeadlineLateness *DeadlineLatenessAgg
}

func NewSession(segmentDuration time.Duration) *Session {
	return &Session{
		AllTiles:   NewSegmentCompletionAgg(),
		FOVTiles:   NewSegmentCompletionAgg(),
		Stale:      NewStaleBytesAgg(),
		Deadlines:  NewTileDeadlineMissAgg(),
		FOVHit:     NewFovHitAgg(),
		FOVGoodput: NewFovGoodputAgg(segmentDuration),
		DeadlineLateness: NewDeadlineLatenessAgg(),
	}
}

// Aggregator for Segment Completion Rate (ALL tiles requested)
// Tracks, per segment, the set of required tiles and the set of tiles
// that arrived on time (before deadline). The completion rate is the
// percentage of segments for which all required tiles arrived on time.
type SegmentCompletionAgg struct {
	required   map[int]map[int]struct{}
	ontime     map[int]map[int]struct{}
	processed  map[int]map[int]struct{}
	finalRatio map[int]float64
	mutex      sync.Mutex
}

func NewSegmentCompletionAgg() *SegmentCompletionAgg {
	return &SegmentCompletionAgg{
		required:   make(map[int]map[int]struct{}),
		ontime:     make(map[int]map[int]struct{}),
		processed:  make(map[int]map[int]struct{}),
		finalRatio: make(map[int]float64),
	}
}

// Aggregator for stale bytes ratio (bytes received after deadline vs total bytes received).
// Guarded by mutex because goroutines update it concurrently.
type StaleBytesAgg struct {
	mutex      sync.Mutex
	lateBytes  uint64
	totalBytes uint64
}

func NewStaleBytesAgg() *StaleBytesAgg {
	return &StaleBytesAgg{}
}

// Add records the amount of bytes received and whether they were late.
func (a *StaleBytesAgg) Add(bytes int, late bool) {
	if bytes <= 0 {
		return
	}
	a.mutex.Lock()
	a.totalBytes += uint64(bytes)
	if late {
		a.lateBytes += uint64(bytes)
	}
	a.mutex.Unlock()
}

// RatioPercent returns the percentage of bytes that arrived after the deadline.
func (a *StaleBytesAgg) RatioPercent() float64 {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	if a.totalBytes == 0 {
		return 0.0
	}
	return 100.0 * float64(a.lateBytes) / float64(a.totalBytes)
}

// TimelyPercent returns the percentage of bytes that arrived on time.
func (a *StaleBytesAgg) TimelyPercent() float64 {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	if a.totalBytes == 0 {
		return 0.0
	}
	onTimeBytes := a.totalBytes - a.lateBytes
	return 100.0 * float64(onTimeBytes) / float64(a.totalBytes)
}

// Aggregator for deadline misses per tile class (FOV vs non-FOV).
type TileDeadlineMissAgg struct {
	mutex       sync.Mutex
	totalFOV    uint64
	missFOV     uint64
	totalNonFOV uint64
	missNonFOV  uint64
}

func NewTileDeadlineMissAgg() *TileDeadlineMissAgg {
	return &TileDeadlineMissAgg{}
}

func (a *TileDeadlineMissAgg) Add(isFOV bool, missed bool) {
	a.mutex.Lock()
	if isFOV {
		a.totalFOV++
		if missed {
			a.missFOV++
		}
	} else {
		a.totalNonFOV++
		if missed {
			a.missNonFOV++
		}
	}
	a.mutex.Unlock()
}

func (a *TileDeadlineMissAgg) Rates() (float64, float64) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	var fovRate float64
	if a.totalFOV > 0 {
		fovRate = 100.0 * float64(a.missFOV) / float64(a.totalFOV)
	}

	var nonFOVRate float64
	if a.totalNonFOV > 0 {
		nonFOVRate = 100.0 * float64(a.missNonFOV) / float64(a.totalNonFOV)
	}

	return fovRate, nonFOVRate
}

// Aggregator for FoV hit rate per segment.
type FovHitAgg struct {
	mutex  sync.Mutex
	total  map[int]uint64
	onTime map[int]uint64
}

func NewFovHitAgg() *FovHitAgg {
	return &FovHitAgg{
		total:  make(map[int]uint64),
		onTime: make(map[int]uint64),
	}
}

func (a *FovHitAgg) Add(segment int, inFOV bool, onTime bool) {
	if !inFOV || segment <= 0 {
		return
	}
	a.mutex.Lock()
	a.total[segment]++
	if onTime {
		a.onTime[segment]++
	}
	a.mutex.Unlock()
}

func (a *FovHitAgg) RateOverall() float64 {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	var total uint64
	var hit uint64
	for seg, cnt := range a.total {
		if cnt == 0 {
			continue
		}
		total += cnt
		hit += a.onTime[seg]
	}
	if total == 0 {
		return 0.0
	}
	return 100.0 * float64(hit) / float64(total)
}

type FovHitSample struct {
	Segment int
	Total   uint64
	OnTime  uint64
	Rate    float64
}

func (a *FovHitAgg) Series(firstSegment, lastSegment int) []FovHitSample {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	if lastSegment < firstSegment {
		return nil
	}
	series := make([]FovHitSample, 0, lastSegment-firstSegment+1)
	for seg := firstSegment; seg <= lastSegment; seg++ {
		total := a.total[seg]
		if total == 0 {
			continue
		}
		onTime := a.onTime[seg]
		rate := 0.0
		if onTime > 0 {
			rate = 100.0 * float64(onTime) / float64(total)
		}
		series = append(series, FovHitSample{
			Segment: seg,
			Total:   total,
			OnTime:  onTime,
			Rate:    rate,
		})
	}
	return series
}

type FovGoodputAgg struct {
	mutex      sync.Mutex
	window     time.Duration
	totalBytes uint64
	buckets    map[int64]uint64
}

func NewFovGoodputAgg(window time.Duration) *FovGoodputAgg {
	return &FovGoodputAgg{
		window:  window,
		buckets: make(map[int64]uint64),
	}
}

func (a *FovGoodputAgg) Add(at time.Duration, bytes int, inFOV bool, onTime bool) {
	if !inFOV || !onTime || bytes <= 0 {
		return
	}
	a.mutex.Lock()
	a.totalBytes += uint64(bytes)
	var bucket int64
	if a.window > 0 {
		bucket = int64(at / a.window)
	}
	a.buckets[bucket] += uint64(bytes)
	a.mutex.Unlock()
}

func (a *FovGoodputAgg) OverallKbps(elapsed time.Duration) float64 {
	if elapsed <= 0 {
		return 0.0
	}
	a.mutex.Lock()
	total := a.totalBytes
	a.mutex.Unlock()
	return (8.0 * float64(total)) / (elapsed.Seconds() * 1000.0)
}

type FovGoodputSample struct {
	WindowStart time.Duration
	WindowEnd   time.Duration
	Bytes       uint64
	Kbps        float64
}

// Aggregator for segment-level "Age/Lateness at deadline":
// per tile lateness = max(0, arrival - deadline), and per segment we keep
// the max lateness among required tiles.
type DeadlineLatenessAgg struct {
	mutex       sync.Mutex
	required    map[int]map[int]struct{}
	processed   map[int]map[int]struct{}
	maxLateness map[int]time.Duration
	maxTile     map[int]int
	final       map[int]DeadlineLatenessSample
}

type DeadlineLatenessSample struct {
	Segment    int
	LastTile   int
	LatenessMs float64
	Required   int
	Processed  int
}

func NewDeadlineLatenessAgg() *DeadlineLatenessAgg {
	return &DeadlineLatenessAgg{
		required:    make(map[int]map[int]struct{}),
		processed:   make(map[int]map[int]struct{}),
		maxLateness: make(map[int]time.Duration),
		maxTile:     make(map[int]int),
		final:       make(map[int]DeadlineLatenessSample),
	}
}

func (a *DeadlineLatenessAgg) SetRequired(segment int, tiles []int) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	set := make(map[int]struct{}, len(tiles))
	for _, tile := range tiles {
		set[tile] = struct{}{}
	}

	a.required[segment] = set
	a.processed[segment] = make(map[int]struct{})
	delete(a.maxLateness, segment)
	delete(a.maxTile, segment)
	delete(a.final, segment)
}

func (a *DeadlineLatenessAgg) Record(segment int, tile int, lateness time.Duration) (time.Duration, bool) {
	if lateness < 0 {
		lateness = 0
	}

	a.mutex.Lock()
	defer a.mutex.Unlock()

	req, ok := a.required[segment]
	if !ok || len(req) == 0 {
		return 0, false
	}
	if _, required := req[tile]; !required {
		if sample, done := a.final[segment]; done {
			return time.Duration(sample.LatenessMs * float64(time.Millisecond)), true
		}
		return a.maxLateness[segment], false
	}

	proc := a.processed[segment]
	if proc == nil {
		proc = make(map[int]struct{})
		a.processed[segment] = proc
	}
	if _, duplicate := proc[tile]; duplicate {
		if sample, done := a.final[segment]; done {
			return time.Duration(sample.LatenessMs * float64(time.Millisecond)), true
		}
		return a.maxLateness[segment], false
	}
	proc[tile] = struct{}{}

	currentMax, hasMax := a.maxLateness[segment]
	if !hasMax || lateness >= currentMax {
		a.maxLateness[segment] = lateness
		a.maxTile[segment] = tile
	}

	if len(proc) == len(req) {
		sample := DeadlineLatenessSample{
			Segment:    segment,
			LastTile:   a.maxTile[segment],
			LatenessMs: float64(a.maxLateness[segment]) / float64(time.Millisecond),
			Required:   len(req),
			Processed:  len(proc),
		}
		a.final[segment] = sample
		return a.maxLateness[segment], true
	}

	return a.maxLateness[segment], false
}

func (a *DeadlineLatenessAgg) Series(firstSegment, lastSegment int) []DeadlineLatenessSample {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	if lastSegment < firstSegment {
		return nil
	}

	series := make([]DeadlineLatenessSample, 0, lastSegment-firstSegment+1)
	for seg := firstSegment; seg <= lastSegment; seg++ {
		req := a.required[seg]
		if len(req) == 0 {
			continue
		}
		sample, ok := a.final[seg]
		if !ok {
			continue
		}
		series = append(series, sample)
	}
	return series
}

func (a *FovGoodputAgg) Series() []FovGoodputSample {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	if len(a.buckets) == 0 {
		return nil
	}
	keys := make([]int64, 0, len(a.buckets))
	for bucket := range a.buckets {
		keys = append(keys, bucket)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	window := a.window
	samples := make([]FovGoodputSample, 0, len(keys))

	if window <= 0 {
		bytes := a.totalBytes
		samples = append(samples, FovGoodputSample{
			WindowStart: 0,
			WindowEnd:   0,
			Bytes:       bytes,
			Kbps:        0.0,
		})
		return samples
	}

	for _, bucket := range keys {
		bytes := a.buckets[bucket]
		start := time.Duration(bucket) * window
		end := start + window
		kbps := 0.0
		if window > 0 {
			kbps = (8.0 * float64(bytes)) / (window.Seconds() * 1000.0)
		}
		samples = append(samples, FovGoodputSample{
			WindowStart: start,
			WindowEnd:   end,
			Bytes:       bytes,
			Kbps:        kbps,
		})
	}

	return samples
}

// SetRequired defines the required tiles for a given segment.
func (a *SegmentCompletionAgg) SetRequired(segment int, tiles []int) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	s := make(map[int]struct{}, len(tiles))
	for _, t := range tiles {
		s[t] = struct{}{}
	}
	a.required[segment] = s
	a.ontime[segment] = make(map[int]struct{})
	a.processed[segment] = make(map[int]struct{})
	delete(a.finalRatio, segment)
}

// Record marks a tile as received on time or not. Only on-time tiles are tracked
// for completion purposes.
func (a *SegmentCompletionAgg) Record(segment, tile int, onTime bool) (float64, bool) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	req, ok := a.required[segment]
	if !ok {
		return -1.0, false
	}

	if _, exists := req[tile]; !exists {
		// Guard against unexpected tiles; treat them as required for completeness.
		req[tile] = struct{}{}
	}

	proc := a.processed[segment]
	if proc == nil {
		proc = make(map[int]struct{})
		a.processed[segment] = proc
	}

	if _, already := proc[tile]; !already {
		proc[tile] = struct{}{}

		if onTime {
			m := a.ontime[segment]
			if m == nil {
				m = make(map[int]struct{})
				a.ontime[segment] = m
			}
			m[tile] = struct{}{}
		}

		if len(proc) == len(req) && len(req) > 0 {
			onTimeCount := 0
			if m := a.ontime[segment]; m != nil {
				onTimeCount = len(m)
			}
			missing := len(req) - onTimeCount
			ratio := float64(missing) / float64(len(req))
			a.finalRatio[segment] = ratio
			return ratio, true
		}
	} else {
		// Tile already processed; ensure we update on-time map if status improved.
		if onTime {
			m := a.ontime[segment]
			if m == nil {
				m = make(map[int]struct{})
				a.ontime[segment] = m
			}
			m[tile] = struct{}{}
		}
	}

	if ratio, ok := a.finalRatio[segment]; ok {
		return ratio, true
	}

	return -1.0, false
}

// TileMissingRatio returns the final ratio for the given segment if known.
// The boolean indicates whether the segment has processed all required tiles.
func (a *SegmentCompletionAgg) TileMissingRatio(segment int) (float64, bool) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	if ratio, ok := a.finalRatio[segment]; ok {
		return ratio, true
	}

	req, ok := a.required[segment]
	if !ok || len(req) == 0 {
		return 0.0, false
	}

	proc := a.processed[segment]
	if proc == nil || len(proc) < len(req) {
		return -1.0, false
	}

	onTimeCount := 0
	if m := a.ontime[segment]; m != nil {
		onTimeCount = len(m)
	}
	missing := len(req) - onTimeCount
	ratio := float64(missing) / float64(len(req))
	a.finalRatio[segment] = ratio
	return ratio, true
}

// Rate computes the percentage of segments in [firstSegment, lastSegment]
// for which all required tiles arrived on time.
func (a *SegmentCompletionAgg) Rate(firstSegment, lastSegment int) float64 {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	if lastSegment < firstSegment {
		return 0.0
	}

	total := 0
	completed := 0
	for seg := firstSegment; seg <= lastSegment; seg++ {
		req, ok := a.required[seg]
		if !ok || len(req) == 0 {
			// If no required tiles were set, treat as not completed.
			continue
		}
		total++
		got := a.ontime[seg]
		all := true
		for t := range req {
			if _, ok := got[t]; !ok {
				all = false
				break
			}
		}
		if all {
			completed++
		}
	}
	if total == 0 {
		return 0.0
	}
	return 100.0 * float64(completed) / float64(total)
}

func WriteFOVDeliverySeries(path string, samples []FovHitSample) {
	if path == "" {
		return
	}
	file, err := os.Create(path)
	if err != nil {
		log.Printf("Failed to create %s: %v", path, err)
		return
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	if _, err := writer.WriteString("segment,fov_tiles,fov_on_time,fov_hit_rate_percent\n"); err != nil {
		log.Printf("Failed to write header to %s: %v", path, err)
		return
	}

	for _, sample := range samples {
		if _, err := fmt.Fprintf(writer, "%d,%d,%d,%.2f\n", sample.Segment, sample.Total, sample.OnTime, sample.Rate); err != nil {
			log.Printf("Failed to write sample to %s: %v", path, err)
			return
		}
	}
}

func WriteFOVGoodputSeries(path string, samples []FovGoodputSample) {
	if path == "" {
		return
	}
	file, err := os.Create(path)
	if err != nil {
		log.Printf("Failed to create %s: %v", path, err)
		return
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	if _, err := writer.WriteString("window_start_s,window_end_s,fov_on_time_bytes,useful_goodput_kbps\n"); err != nil {
		log.Printf("Failed to write header to %s: %v", path, err)
		return
	}

	for _, sample := range samples {
		startSec := sample.WindowStart.Seconds()
		endSec := sample.WindowEnd.Seconds()
		if _, err := fmt.Fprintf(writer, "%.3f,%.3f,%d,%.2f\n", startSec, endSec, sample.Bytes, sample.Kbps); err != nil {
			log.Printf("Failed to write sample to %s: %v", path, err)
			return
		}
	}
}

func WriteDeadlineLatenessSeries(path string, samples []DeadlineLatenessSample) {
	if path == "" {
		return
	}
	file, err := os.Create(path)
	if err != nil {
		log.Printf("Failed to create %s: %v", path, err)
		return
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	if _, err := writer.WriteString("segment,last_tile,lateness_ms,required_tiles,processed_tiles\n"); err != nil {
		log.Printf("Failed to write header to %s: %v", path, err)
		return
	}

	for _, sample := range samples {
		if _, err := fmt.Fprintf(writer, "%d,%d,%.3f,%d,%d\n",
			sample.Segment, sample.LastTile, sample.LatenessMs, sample.Required, sample.Processed); err != nil {
			log.Printf("Failed to write sample to %s: %v", path, err)
			return
		}
	}
}
