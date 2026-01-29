package session

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"main/src/model"
	"main/src/test_client/fov"
	"main/src/test_client/metrics"
	"main/src/test_client/netstats"
)

// RequestSender is satisfied by the QUIC client and allows the scheduler
// to remain agnostic to the underlying transport.
type RequestSender interface {
	Request(model.VideoPacketRequest, time.Duration) *model.VideoPacketResponse
}

// Options describes runtime parameters independent from environment derived inputs.
type Options struct {
	Parallelism     int
	BaseLatency     time.Duration
	SegmentDuration time.Duration
	FirstSegment    int
	LastSegment     int
	FirstTile       int
	LastTile        int
}

// Environment groups filesystem paths and FOV configuration needed during a session.
type Environment struct {
	FOVTracePath    string
	FOVTraceFPS     int
	StatisticsPath  string
	SummaryPath     string
	FOVDeliveryPath string
	FOVGoodputPath  string
}

type TestSession struct {
	client        RequestSender
	env           Environment
	opts          Options
	statsLogger   *metrics.StatisticsLogger
	summaryLogger *metrics.SummaryLogger
	playback      *PlaybackSimulator
	metrics       *metrics.Session
	collector     *netstats.StatsCollector
	abr           ABRController
	semaphore     Semaphore
	fovTrace      *fov.FOVTrace

	lastDownloadedSegment atomic.Int32
	tileUniverse          []int
}

func NewTestSession(client RequestSender, env Environment, opts Options) *TestSession {
	statsLogger := metrics.NewStatisticsLogger(env.StatisticsPath)
	summaryLogger := metrics.NewSummaryLogger(env.SummaryPath)
	playback := NewPlaybackSimulator(opts.SegmentDuration, opts.BaseLatency, opts.FirstSegment, opts.LastSegment)
	metricSet := metrics.NewSession(opts.SegmentDuration)
	collector := netstats.New(opts.LastSegment - opts.FirstSegment + 1)
	semaphore := NewSemaphore(opts.Parallelism)

	session := &TestSession{
		client:        client,
		env:           env,
		opts:          opts,
		statsLogger:   statsLogger,
		summaryLogger: summaryLogger,
		playback:      playback,
		metrics:       metricSet,
		collector:     collector,
		abr:           NewDefaultABRController(),
		semaphore:     semaphore,
		tileUniverse:  buildTileUniverse(opts.FirstTile, opts.LastTile),
	}
	session.lastDownloadedSegment.Store(int32(opts.FirstSegment - 1))
	return session
}

func (s *TestSession) Run() error {
	defer s.statsLogger.Close()
	defer s.summaryLogger.Close()

	s.playback.Start()

	if err := s.loadFOVTrace(); err != nil {
		log.Printf("Failed to load FOV trace from %s: %v (continuing without FOV prioritisation)", s.env.FOVTracePath, err)
	}

	startTime := time.Now()
	scheduler := NewTileScheduler(s.client, s.playback, s.collector, s.metrics, s.statsLogger, s.semaphore, startTime, &s.lastDownloadedSegment, s.abr)

	log.Printf("Starting test iteration for segments %d to %d (tiles %d to %d)", s.opts.FirstSegment, s.opts.LastSegment, s.opts.FirstTile, s.opts.LastTile)
	fmt.Printf("Test started with parallelism = %d\n", s.opts.Parallelism)

	for segmentID := s.opts.FirstSegment; segmentID <= s.opts.LastSegment; segmentID++ {
		s.processSegment(segmentID, scheduler)
	}

	log.Println("Waiting for all goroutines to finish...")
	scheduler.Wait()
	log.Println("All goroutines completed.")
	fmt.Println("Test iteration complete.")

	s.finalize(startTime, scheduler.FirstRequestTime())
	return nil
}

func (s *TestSession) processSegment(segmentID int, scheduler *TileScheduler) {
	log.Printf("Processing segment %d", segmentID)
	avgThroughput := s.collector.AvgThroughput()
	bufferLevel := s.playback.GetBufferLevel(int(s.lastDownloadedSegment.Load()))
	fovBitrate := s.abr.Select(avgThroughput, bufferLevel, true)
	log.Printf("ABR: Average Throughput = %.2f, Buffer Level = %.2f s, Selected Bitrate (FOV tiles) = %d", avgThroughput, bufferLevel.Seconds(), fovBitrate)

	s.playback.WaitUntilWithinPrefetchWindow(segmentID)
	timeBudget := s.playback.GetTimeToReceive(segmentID)
	if timeBudget <= 0 {
		timeBudget = s.opts.SegmentDuration
	}
	maxAhead := 3 * s.opts.SegmentDuration
	if timeBudget > maxAhead {
		timeBudget = maxAhead
	}
	timeBudget += s.opts.SegmentDuration
	segmentDeadline := time.Now().Add(timeBudget)

	s.metrics.AllTiles.SetRequired(segmentID, s.tileUniverse)
	var fovTiles []int
	if s.fovTrace != nil {
		fovTiles = filterTilesInRange(s.fovTrace.TilesForSegment(segmentID), s.opts.FirstTile, s.opts.LastTile)
	}
	s.metrics.FOVTiles.SetRequired(segmentID, fovTiles)

	scheduler.ScheduleSegment(segmentID, segmentDeadline, avgThroughput, bufferLevel, s.opts.FirstTile, s.opts.LastTile, s.fovTrace)
}

func (s *TestSession) finalize(startTime time.Time, firstRequestTime time.Time) {
	elapsed := time.Since(startTime)

	var joinLatency time.Duration
	if !firstRequestTime.IsZero() {
		playbackStart := s.playback.GetPlaybackStartTime()
		joinLatency = playbackStart.Sub(firstRequestTime)
		if joinLatency < 0 {
			joinLatency = 0
		}
		log.Printf("Join latency: %d ms", joinLatency.Milliseconds())
	} else {
		log.Println("Join latency: first request timestamp not captured")
	}

	completionRate := s.metrics.AllTiles.Rate(s.opts.FirstSegment, s.opts.LastSegment)
	log.Printf("Segment completion rate (ALL tiles): %.2f%%", completionRate)

	lastFOVSegment := s.lastFOVSegment()
	fovCompletionRate := -1.0
	if lastFOVSegment > 0 {
		fovCompletionRate = s.metrics.FOVTiles.Rate(s.opts.FirstSegment, lastFOVSegment)
		log.Printf("Segment completion rate (FOV tiles): %.2f%%", fovCompletionRate)
	} else {
		log.Println("Segment completion rate (FOV tiles): N/A (no FOV trace)")
	}

	staleRatio := s.metrics.Stale.RatioPercent()
	log.Printf("Stale bytes ratio: %.2f%%", staleRatio)

	fovMissRate, nonFOVMissRate := s.metrics.Deadlines.Rates()
	log.Printf("Deadline miss rate (FOV tiles): %.2f%%", fovMissRate)
	log.Printf("Deadline miss rate (non-FOV tiles): %.2f%%", nonFOVMissRate)

	fovHitRate := s.metrics.FOVHit.RateOverall()
	log.Printf("FoV hit rate (delivery): %.2f%%", fovHitRate)

	fovGoodputRate := s.metrics.FOVGoodput.OverallKbps(elapsed)
	log.Printf("Useful goodput (FoV): %.2f kbps", fovGoodputRate)

	if s.summaryLogger != nil {
		s.summaryLogger.LogSession(joinLatency, completionRate, fovCompletionRate, staleRatio, fovMissRate, nonFOVMissRate, fovHitRate, fovGoodputRate)
	}

	if s.env.FOVDeliveryPath != "" {
		samples := s.metrics.FOVHit.Series(s.opts.FirstSegment, s.opts.LastSegment)
		metrics.WriteFOVDeliverySeries(s.env.FOVDeliveryPath, samples)
	}

	if s.env.FOVGoodputPath != "" {
		goodputSamples := s.metrics.FOVGoodput.Series()
		metrics.WriteFOVGoodputSeries(s.env.FOVGoodputPath, goodputSamples)
	}
}

func (s *TestSession) lastFOVSegment() int {
	if s.fovTrace == nil {
		return 0
	}
	last := s.fovTrace.MaxSegment()
	if last > s.opts.LastSegment {
		last = s.opts.LastSegment
	}
	if last < s.opts.FirstSegment {
		return 0
	}
	return last
}

func (s *TestSession) loadFOVTrace() error {
	trace, err := fov.LoadFOVTrace(s.env.FOVTracePath, s.env.FOVTraceFPS, s.opts.SegmentDuration)
	if err != nil {
		s.fovTrace = nil
		return err
	}
	s.fovTrace = trace
	log.Printf("Loaded FOV trace: fps=%d, segments=%d", s.env.FOVTraceFPS, s.fovTrace.MaxSegment())
	return nil
}

func buildTileUniverse(firstTile, lastTile int) []int {
	size := lastTile - firstTile + 1
	if size <= 0 {
		return nil
	}
	tiles := make([]int, 0, size)
	for tileID := firstTile; tileID <= lastTile; tileID++ {
		tiles = append(tiles, tileID)
	}
	return tiles
}

func filterTilesInRange(tiles []int, min, max int) []int {
	if len(tiles) == 0 {
		return nil
	}
	filtered := make([]int, 0, len(tiles))
	for _, tile := range tiles {
		if tile >= min && tile <= max {
			filtered = append(filtered, tile)
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	return filtered
}
