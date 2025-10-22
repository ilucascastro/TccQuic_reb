// Basic client for testing the server functionality

package test_client

import (
	"fmt"
	"log"
	"main/src/model"
	"main/src/test_client/netstats"
	"os"
	"strconv"
	"sync"
	"sync/atomic" // Adiciona o import para sync/atomic
	"time"

	"github.com/google/uuid"
)

// If pipeline = true, use the same stream for all requests.
// If pipeline = false, use one stream for each request.
const pipeline = false

// Proportion of medium priority
const mediumPriorityRatio = 0.0

// Proportion of high priority
const highPriorityRatio = 0.3

const (
	defaultFOVTracePath = "data/user_fov.csv"
	defaultFOVTraceFPS  = 30
)

// Aggregator for Segment Completion Rate (ALL tiles requested)
// Tracks, per segment, the set of required tiles and the set of tiles
// that arrived on time (before deadline). The completion rate is the
// percentage of segments for which all required tiles arrived on time.
type segmentCompletionAgg struct {
    required map[int]map[int]struct{}
    ontime   map[int]map[int]struct{}
    processed map[int]map[int]struct{}
    finalRatio map[int]float64
    mutex    sync.Mutex
}

func newSegmentCompletionAgg() *segmentCompletionAgg {
    return &segmentCompletionAgg{
        required:  make(map[int]map[int]struct{}),
        ontime:    make(map[int]map[int]struct{}),
        processed: make(map[int]map[int]struct{}),
        finalRatio: make(map[int]float64),
    }
}

// Aggregator for stale bytes ratio (bytes received after deadline vs total bytes received).
// Guarded by mutex because goroutines update it concurrently.
type staleBytesAgg struct {
    mutex      sync.Mutex
    lateBytes  uint64
    totalBytes uint64
}

func newStaleBytesAgg() *staleBytesAgg {
    return &staleBytesAgg{}
}

// Add records the amount of bytes received and whether they were late.
func (a *staleBytesAgg) Add(bytes int, late bool) {
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
func (a *staleBytesAgg) RatioPercent() float64 {
    a.mutex.Lock()
    defer a.mutex.Unlock()
    if a.totalBytes == 0 {
        return 0.0
    }
    return 100.0 * float64(a.lateBytes) / float64(a.totalBytes)
}

// SetRequired defines the required tiles for a given segment.
func (a *segmentCompletionAgg) SetRequired(segment int, tiles []int) {
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
func (a *segmentCompletionAgg) Record(segment, tile int, onTime bool) (float64, bool) {
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
func (a *segmentCompletionAgg) TileMissingRatio(segment int) (float64, bool) {
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
func (a *segmentCompletionAgg) Rate(firstSegment, lastSegment int) float64 {
    a.mutex.Lock()
    defer a.mutex.Unlock()

    if lastSegment < firstSegment {
        return 0.0
    }

    total := 0
    completed := 0
    for seg := firstSegment; seg <= lastSegment; seg++ {
        total++
        req, ok := a.required[seg]
        if !ok || len(req) == 0 {
            // If no required tiles were set, treat as not completed.
            continue
        }
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

func StartTestClient(serverURL string, serverPort int, parallelism int, baseLatencyMs int) {
	client := NewClient(ClientOptions{
		Pipeline:   pipeline,
		ServerURL:  serverURL,
		ServerPort: serverPort,
	})

	log.Println("Base latency =", baseLatencyMs)

	err := client.Connect()
	if err != nil {
		log.Println("failed to connect")
		return
	}

	segmentDuration := 1 * time.Second
	fovPath := os.Getenv("FOV_TRACE_PATH")
	if fovPath == "" {
		fovPath = defaultFOVTracePath
	}

	fps := defaultFOVTraceFPS
	if envFPS := os.Getenv("FOV_TRACE_FPS"); envFPS != "" {
		if parsed, parseErr := strconv.Atoi(envFPS); parseErr == nil && parsed > 0 {
			fps = parsed
		} else {
			log.Printf("Invalid FOV_TRACE_FPS=%q, falling back to default %d", envFPS, defaultFOVTraceFPS)
		}
	}

	var fovTrace *FOVTrace
	if trace, traceErr := LoadFOVTrace(fovPath, fps, segmentDuration); traceErr != nil {
		log.Printf("Failed to load FOV trace from %s: %v (continuing without FOV prioritisation)", fovPath, traceErr)
	} else {
		fovTrace = trace
		log.Printf("Loaded FOV trace: fps=%d, segments=%d", fps, fovTrace.MaxSegment())
	}

	statisticsPath := fmt.Sprintf("statistics-%d.csv", os.Getpid())
	summaryPath := fmt.Sprintf("statistics-summary-%d.csv", os.Getpid())

	statisticsLogger := NewStatisticsLogger(statisticsPath)
	summaryLogger := NewSummaryLogger(summaryPath)
	runTestIteration(client, parallelism, baseLatencyMs, statisticsLogger, summaryLogger, segmentDuration, fovTrace)
	statisticsLogger.Close()
	summaryLogger.Close()
}

func runTestIteration(client *Client, parallelism int, baseLatencyMs int,
	statisticsLogger *StatisticsLogger, summaryLogger *SummaryLogger, segmentDuration time.Duration, fovTrace *FOVTrace) {
	var wg sync.WaitGroup

	startTime := time.Now()

	baseLatency := time.Duration(baseLatencyMs) * time.Millisecond
	firstTile := 100
	lastTile := 177

	playbackSimulator := NewPlaybackSimulator(
		segmentDuration,
		baseLatency,
		firstTile,
		lastTile,
	)

	parallelismSemaphore := NewSemaphore(parallelism)

	log.Printf("Starting test iteration for tiles %d to %d", firstTile, lastTile)
	fmt.Printf("Test started with parallelism = %d\n", parallelism)

	playbackSimulator.Start()

	var firstRequestOnce sync.Once
	var firstRequestTime time.Time

	const totalTimeSegments = 120

	collector := netstats.New(totalTimeSegments)
	currentBitrate := model.HIGH_BITRATE
	var lastDownloadedSegment atomic.Int32
	lastDownloadedSegment.Store(int32(firstTile - 1))

	agg := newSegmentCompletionAgg()
	aggFOV := newSegmentCompletionAgg()
	staleAgg := newStaleBytesAgg()

	lastFOVSegment := 0
	if fovTrace != nil {
		lastFOVSegment = fovTrace.MaxSegment()
		if lastFOVSegment > totalTimeSegments {
			lastFOVSegment = totalTimeSegments
		}
		if lastFOVSegment < 0 {
			lastFOVSegment = 0
		}
	}
	for segIdx := 1; segIdx <= totalTimeSegments; segIdx++ {
		var tiles []int
		if fovTrace != nil {
			tiles = fovTrace.TilesForSegment(segIdx)
		}
		aggFOV.SetRequired(segIdx, tiles)
	}

	for tileID := firstTile; tileID <= lastTile; tileID++ {
		log.Printf("Processing tile %d", tileID)

		avgThroughput := collector.AvgThroughput()
		bufferLevel := playbackSimulator.GetBufferLevel(int(lastDownloadedSegment.Load()))
		currentBitrate = adaptBitrateWithBuffer(avgThroughput, bufferLevel)
		log.Printf("ABR: Average Throughput = %.2f, Buffer Level = %.2f s, Selected Bitrate = %d", avgThroughput, bufferLevel.Seconds(), currentBitrate)

		playbackSimulator.WaitUntilWithinPrefetchWindow(tileID)
		timeBudget := playbackSimulator.GetTimeToReceive(tileID)
		if timeBudget <= 0 {
			timeBudget = segmentDuration
		}
		maxAhead := 3 * segmentDuration
		if timeBudget > maxAhead {
			timeBudget = maxAhead
		}
		timeBudget += segmentDuration
		tileDeadline := time.Now().Add(timeBudget)
		segmentBitrate := currentBitrate

		requiredSegments := make([]int, totalTimeSegments)
		for k := 1; k <= totalTimeSegments; k++ {
			requiredSegments[k-1] = k
		}
		agg.SetRequired(tileID, requiredSegments)

		for timeSegment := 1; timeSegment <= totalTimeSegments; timeSegment++ {
			inFOV := fovTrace != nil && fovTrace.Contains(timeSegment, tileID)

			priority := model.LOW_PRIORITY
			requestBitrate := model.LOW_BITRATE
			if inFOV {
				priority = model.HIGH_PRIORITY
				requestBitrate = segmentBitrate
			}

			parallelismSemaphore.Acquire()
			wg.Add(1)

			go func(tileID, timeSegment int, deadline time.Time, bitrate model.Bitrate, priority model.Priority, inFOV bool) {
				defer func() {
					parallelismSemaphore.Release()
					wg.Done()
				}()

				remaining := time.Until(deadline)
				if remaining <= 0 {
					ratio, complete := agg.Record(tileID, timeSegment, false)
					tmrValue := -1.0
					if complete {
						tmrValue = ratio
					}
					aggFOV.Record(timeSegment, tileID, false)
					if statisticsLogger != nil {
						bufferSec := playbackSimulator.GetBufferLevel(int(lastDownloadedSegment.Load())).Seconds()
						statisticsLogger.Log(time.Since(startTime), model.VideoPacketRequest{
							ID:       uuid.Nil,
							Priority: priority,
							Bitrate:  bitrate,
							Segment:  tileID,
							Tile:     timeSegment,
							Timeout:  0,
						}, 0, true, true, false, 0.0, bufferSec, tmrValue, inFOV)
					}
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
					Segment:  tileID,
					Tile:     timeSegment,
					Timeout:  timeoutMs,
				}

				fmt.Printf("Sending request for tile %d, segment %d (priority=%d, FOV=%t)\n", tileID, timeSegment, priority, inFOV)

				firstRequestOnce.Do(func() { firstRequestTime = time.Now() })

				sendBufferSec := playbackSimulator.GetBufferLevel(int(lastDownloadedSegment.Load())).Seconds()
				collector.RecordSend(request.ID)

				requestTime := time.Since(startTime)
				response := client.Request(request, remaining)
				responseTime := time.Since(startTime)

				var timedOut bool
				var late bool
				if response == nil {
					fmt.Printf("Timeout: no response for tile %d, segment %d\n", tileID, timeSegment)
					timedOut = true
					instaThroughput = 0.0
				} else {
					if len(response.Data) == 0 {
						log.Panicf("Empty response for (%d, %d)", tileID, timeSegment)
					}
					bytesReceived := len(response.Data)
					_, instaThroughput = collector.RecordRecv(request.ID, bytesReceived)

					late = time.Now().After(deadline)
					staleAgg.Add(bytesReceived, late)

					if late {
						fmt.Printf("Late response for tile %d, segment %d\n", tileID, timeSegment)
						timedOut = true
					} else {
						fmt.Printf("Received response for tile %d, segment %d\n", tileID, timeSegment)
						timedOut = false
					}
				}

				onTime := (response != nil) && (!timedOut)
				ratio, complete := agg.Record(tileID, timeSegment, onTime)
				tmrValue := -1.0
				if complete {
					tmrValue = ratio
				}
				aggFOV.Record(timeSegment, tileID, onTime)

				if response != nil {
					for {
						oldValue := lastDownloadedSegment.Load()
						if int32(tileID) > oldValue {
							if lastDownloadedSegment.CompareAndSwap(oldValue, int32(tileID)) {
								break
							}
						} else {
							break
						}
					}
				}

				if statisticsLogger != nil {
					statisticsLogger.Log(requestTime, request,
						responseTime-requestTime, timedOut, false, !timedOut, instaThroughput, sendBufferSec, tmrValue, inFOV)
				}
			}(tileID, timeSegment, tileDeadline, requestBitrate, priority, inFOV)
		}
	}

	log.Println("Waiting for all goroutines to finish...")
	wg.Wait()
	log.Println("All goroutines completed.")
	fmt.Println("Test iteration complete.")

	var joinLatency time.Duration
	if !firstRequestTime.IsZero() {
		playbackStart := playbackSimulator.GetPlaybackStartTime()
		joinLatency = playbackStart.Sub(firstRequestTime)
		if joinLatency < 0 {
			joinLatency = 0
		}
		log.Printf("Join latency: %d ms", joinLatency.Milliseconds())
	} else {
		log.Println("Join latency: first request timestamp not captured")
	}

	completionRate := agg.Rate(firstTile, lastTile)
	log.Printf("Segment completion rate (ALL tiles): %.2f%%", completionRate)

	fovCompletionRate := -1.0
	if lastFOVSegment > 0 {
		fovCompletionRate = aggFOV.Rate(1, lastFOVSegment)
		log.Printf("Segment completion rate (FOV tiles): %.2f%%", fovCompletionRate)
	} else {
		log.Println("Segment completion rate (FOV tiles): N/A (no FOV trace)")
	}

	staleRatio := staleAgg.RatioPercent()
	log.Printf("Stale bytes ratio: %.2f%%", staleRatio)

	if summaryLogger != nil {
		summaryLogger.LogSession(joinLatency, completionRate, fovCompletionRate, staleRatio)
	}
}

// metricas de rede (vazão instantanea + media)
// tamanho do buffer
// identificação do tile + segmento + prioridade (fov)
func AdaptationAlg() {

}

// Definir uma estrutura para associar Bitrate com seu Threshold (vazão mínima)
type BitrateInfo struct {
	Bitrate   model.Bitrate
	Threshold float64
}

// Slice de BitrateInfo, ordenado do maior Threshold para o menor.
// Isso permite que o algoritmo selecione a maior taxa de bits que a vazão atual suporta.
var availableBitrates = []BitrateInfo{
	{Bitrate: model.HIGH_BITRATE, Threshold: 60000.0},
	{Bitrate: model.MEDIUM_BITRATE, Threshold: 30000.0},
	{Bitrate: model.LOW_BITRATE, Threshold: 0.0}, // LOW_BITRATE é o fallback se a vazão for muito baixa
}

// Comentado: adaptBitrate decide a taxa de bits com base na vazão média e nos bitrates disponíveis.
// func adaptBitrate(avgThroughput float64) model.Bitrate {
// 	for _, brInfo := range availableBitrates {
// 		if avgThroughput >= brInfo.Threshold {
// 			return brInfo.Bitrate
// 		}
// 	}
// 	// Fallback: Se por algum motivo nenhum threshold for atingido (o que não deve acontecer
// 	// com o LOW_BITRATE.Threshold = 0), retorna a menor taxa de bits.
// 	return model.LOW_BITRATE
// }

// adaptBitrateWithBuffer decide a taxa de bits com base na vazão média, buffer level e nos bitrates disponíveis.
func adaptBitrateWithBuffer(avgThroughput float64, bufferLevel time.Duration) model.Bitrate {
	// Definir os limites do buffer. Estes valores podem ser ajustados.
	const minBufferLevel = 2 * time.Second   // Exemplo: se o buffer for menor que 2 segundos, priorizar o preenchimento
	const maxBufferLevel = 10 * time.Second // Exemplo: se o buffer for maior que 10 segundos, pode tentar bitrate mais alto

	// Lógica básica:
	// 1. Se o buffer estiver muito baixo, priorizar um bitrate mais baixo para encher o buffer rapidamente.
	if bufferLevel < minBufferLevel {
		log.Printf("ABR (Buffer): Buffer level (%v) is below minimum (%v). Forcing LOW_BITRATE.", bufferLevel, minBufferLevel)
		return model.LOW_BITRATE
	}

	// 2. Se o buffersaudáve estiver l (entre min e max), usar a lógica de vazão.
	// 3. Se o buffer estiver cheio, podemos ser mais agressivos com o bitrate (ou simplesmente usar a lógica de vazão).

	// Lógica de vazão adaptada (a mesma de adaptBitrate, mas agora com a consideração do buffer)
	for _, brInfo := range availableBitrates {
		if avgThroughput >= brInfo.Threshold {
			return brInfo.Bitrate
		}
	}

	return model.LOW_BITRATE
}
