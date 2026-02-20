package session

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"main/src/model"
	"main/src/test_client/fov"
	"main/src/test_client/metrics"
	"main/src/test_client/netstats"
)

// TileScheduler owns the goroutine fan-out per segment. The session prepares
// per-segment context (deadline, ABR decisions, required tiles) and invokes
// ScheduleSegment, while the scheduler handles request/response bookkeeping.
type TileScheduler struct {
	client                RequestSender
	playback              *PlaybackSimulator
	collector             *netstats.StatsCollector
	metrics               *metrics.Session
	statsLogger           *metrics.StatisticsLogger
	startTime             time.Time
	firstRequestOnce      sync.Once
	firstRequestTime      time.Time
	lastDownloadedSegment *atomic.Int32
	sem                   Semaphore
	wg                    sync.WaitGroup
}

func NewTileScheduler(client RequestSender, playback *PlaybackSimulator, collector *netstats.StatsCollector,
	metrics *metrics.Session, statsLogger *metrics.StatisticsLogger, sem Semaphore, startTime time.Time,
	lastDownloadedSegment *atomic.Int32) *TileScheduler {
	return &TileScheduler{
		client:                client,
		playback:              playback,
		collector:             collector,
		metrics:               metrics,
		statsLogger:           statsLogger,
		startTime:             startTime,
		lastDownloadedSegment: lastDownloadedSegment,
		sem:                   sem,
	}
}

func (s *TileScheduler) ScheduleSegment(segmentID int, deadline time.Time, cfg SegmentConfig,
	firstTile, lastTile int, fovTrace *fov.FOVTrace) {
	for tileID := firstTile; tileID <= lastTile; tileID++ {
		inFOV := fovTrace != nil && fovTrace.Contains(segmentID, tileID)

		priority := model.LOW_PRIORITY
		if inFOV {
			priority = model.HIGH_PRIORITY
		}

		requestBitrate := cfg.NonFOVBitrate
		if inFOV {
			requestBitrate = cfg.FOVBitrate
		}

		s.sem.Acquire()
		s.wg.Add(1)

		go s.handleTile(segmentID, tileID, deadline, requestBitrate, priority, inFOV)
	}
}

func (s *TileScheduler) handleTile(segmentID, tileID int, deadline time.Time, bitrate model.Bitrate, priority model.Priority, inFOV bool) {
	defer func() {
		s.sem.Release()
		s.wg.Done()
	}()

	remaining := time.Until(deadline)
	if remaining <= 0 {
		s.registerTimeout(segmentID, tileID, priority, bitrate, inFOV)
		return
	}

	timeoutMs := int(remaining / time.Millisecond)
	if timeoutMs <= 0 {
		timeoutMs = 1
	}
	var instaThroughput float64

	request := model.VideoPacketRequest{
		ID:       uuid.Must(uuid.New(), nil),
		Priority: priority,
		Bitrate:  bitrate,
		Segment:  segmentID,
		Tile:     tileID,
		Timeout:  timeoutMs,
	}

	fmt.Printf("Sending request for segment %d, tile %d (priority=%d, FOV=%t)\n", segmentID, tileID, priority, inFOV)

	s.firstRequestOnce.Do(func() { s.firstRequestTime = time.Now() })

	sendBufferSec := s.playback.GetBufferLevel(int(s.lastDownloadedSegment.Load())).Seconds()
	s.collector.RecordSend(request.ID)

	requestTime := time.Since(s.startTime)
	response := s.client.Request(request, remaining)
	responseTime := time.Since(s.startTime)

	bytesReceived := 0
	var timedOut bool
	if response == nil {
		fmt.Printf("Timeout: no response for segment %d, tile %d\n", segmentID, tileID)
		timedOut = true
		instaThroughput = 0.0
	} else {
		if len(response.Data) == 0 {
			log.Panicf("Empty response for (%d, %d)", segmentID, tileID)
		}
		bytesReceived = len(response.Data)
		_, instaThroughput = s.collector.RecordRecv(request.ID, bytesReceived)

		late := time.Now().After(deadline)
		s.metrics.Stale.Add(bytesReceived, late)

		if late {
			fmt.Printf("Late response for segment %d, tile %d\n", segmentID, tileID)
			timedOut = true
		} else {
			fmt.Printf("Received response for segment %d, tile %d\n", segmentID, tileID)
			timedOut = false
		}
	}

	onTime := (response != nil) && (!timedOut)
	s.metrics.FOVHit.Add(segmentID, inFOV, onTime)
	s.metrics.FOVGoodput.Add(responseTime, bytesReceived, inFOV, onTime)
	s.metrics.Deadlines.Add(inFOV, !onTime)
	ratio, complete := s.metrics.AllTiles.Record(segmentID, tileID, onTime)
	tmrValue := -1.0
	if complete {
		tmrValue = ratio
	}
	if inFOV {
		s.metrics.FOVTiles.Record(segmentID, tileID, onTime)
	}

	if response != nil {
		for {
			oldValue := s.lastDownloadedSegment.Load()
			if int32(segmentID) > oldValue {
				if s.lastDownloadedSegment.CompareAndSwap(oldValue, int32(segmentID)) {
					break
				}
			} else {
				break
			}
		}
	}

	if s.statsLogger != nil {
		s.statsLogger.Log(requestTime, request,
			responseTime-requestTime, timedOut, false, !timedOut, instaThroughput, sendBufferSec, tmrValue, inFOV, onTime)
	}
}

func (s *TileScheduler) registerTimeout(segmentID, tileID int, priority model.Priority, bitrate model.Bitrate, inFOV bool) {
	ratio, complete := s.metrics.AllTiles.Record(segmentID, tileID, false)
	tmrValue := -1.0
	if complete {
		tmrValue = ratio
	}
	if inFOV {
		s.metrics.FOVTiles.Record(segmentID, tileID, false)
	}
	s.metrics.Deadlines.Add(inFOV, true)
	s.metrics.FOVHit.Add(segmentID, inFOV, false)
	if s.statsLogger != nil {
		bufferSec := s.playback.GetBufferLevel(int(s.lastDownloadedSegment.Load())).Seconds()
		s.statsLogger.Log(time.Since(s.startTime), model.VideoPacketRequest{
			ID:       uuid.Nil,
			Priority: priority,
			Bitrate:  bitrate,
			Segment:  segmentID,
			Tile:     tileID,
			Timeout:  0,
		}, 0, true, true, false, 0.0, bufferSec, tmrValue, inFOV, false)
	}
}

func (s *TileScheduler) Wait() {
	s.wg.Wait()
}

func (s *TileScheduler) FirstRequestTime() time.Time {
	return s.firstRequestTime
}
