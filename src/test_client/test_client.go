// Basic client for testing the server functionality

package test_client

import (
	"fmt"
	"log"
	"main/src/model"
	"main/src/test_client/netstats"
	"os"
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

    statisticsPath := fmt.Sprintf("statistics-%d.csv", os.Getpid())
    summaryPath := fmt.Sprintf("statistics-summary-%d.csv", os.Getpid())

    statisticsLogger := NewStatisticsLogger(statisticsPath)
    summaryLogger := NewSummaryLogger(summaryPath)
    runTestIteration(client, parallelism, baseLatencyMs, statisticsLogger, summaryLogger)
    statisticsLogger.Close()
    summaryLogger.Close()
}

func runTestIteration(client *Client, parallelism int, baseLatencyMs int,
    statisticsLogger *StatisticsLogger, summaryLogger *SummaryLogger) {
    var wg sync.WaitGroup

    startTime := time.Now()

	segmentDuration := 1 * time.Second
	baseLatency := time.Duration(baseLatencyMs) * time.Millisecond
	firstSegment := 100
	lastSegment := 177
	playbackSimulator := NewPlaybackSimulator(
		segmentDuration,
		baseLatency,
		firstSegment,
		lastSegment,
	)

	//counter := 0
	//counterMediumPriority := 0
	//counterHighPriority := 0
	// Comentado: Variáveis de contador para prioridade, não usadas no ABR v1.0
	// counter := 0
	// counterMediumPriority := 0
	// counterHighPriority := 0

	parallelismSemaphore := NewSemaphore(parallelism)

	log.Printf("Starting test iteration for segments %d to %d", firstSegment, lastSegment)
	fmt.Printf("Test started with parallelism = %d\n", parallelism)

    playbackSimulator.Start()

    // Captura do timestamp do primeiro request de mídia
    var firstRequestOnce sync.Once
    var firstRequestTime time.Time

	// Inicia o pacote de coleta de dados da rede com uma window size
	collector := netstats.New(120)
	currentBitrate := model.HIGH_BITRATE // Inicializa a taxa de bits com o valor mais alto
	var lastDownloadedSegment atomic.Int32 // Declara como atomic.Int32
	lastDownloadedSegment.Store(int32(firstSegment - 1)) // Inicializa de forma atômica

    // Aggregator para Segment Completion Rate (ALL tiles)
    agg := newSegmentCompletionAgg()
    staleAgg := newStaleBytesAgg()

	for iSegment := firstSegment; iSegment <= lastSegment; iSegment++ {
		log.Printf("Processing segment %d", iSegment)

		// ABR logic: Adapt bitrate based on average throughput and buffer level
		avgThroughput := collector.AvgThroughput()
		// Lê o valor de lastDownloadedSegment de forma atômica
		bufferLevel := playbackSimulator.GetBufferLevel(int(lastDownloadedSegment.Load()))
		currentBitrate = adaptBitrateWithBuffer(avgThroughput, bufferLevel)
		log.Printf("ABR: Average Throughput = %.2f, Buffer Level = %.2f s, Selected Bitrate = %d", avgThroughput, bufferLevel.Seconds(), currentBitrate)

        // Em vez de esperar o início da reprodução do próprio segmento,
        // aguardamos apenas até que o segmento esteja dentro da janela de
        // pré-buffer permitida. Isso permite pré-carregar e construir buffer.
        playbackSimulator.WaitUntilWithinPrefetchWindow(iSegment)
		timeBudget := playbackSimulator.GetTimeToReceive(iSegment)
		if timeBudget <= 0 {
			timeBudget = segmentDuration
		}
		maxAhead := 3 * segmentDuration
		if timeBudget > maxAhead {
			timeBudget = maxAhead
		}
		timeBudget += segmentDuration
		segmentDeadline := time.Now().Add(timeBudget)
		segmentBitrate := currentBitrate

        // Define required tiles for this segment (ALL tiles requested: 1..120)
        requiredTiles := make([]int, 120)
        for k := 1; k <= 120; k++ {
            requiredTiles[k-1] = k
        }
        agg.SetRequired(iSegment, requiredTiles)

		for iTile := 1; iTile <= 120; iTile++ {
			tile, segment := iTile, iSegment

			//priority := model.LOW_PRIORITY
			priority := model.LOW_PRIORITY // Prioridade fixada para LOW no ABR v1.0

			// Comentado: Lógica de classificação de prioridade, não usada no ABR v1.0
			// if float64(counterHighPriority)/float64(counter+1) < highPriorityRatio {
			// 	priority = model.HIGH_PRIORITY
			// 	counterHighPriority++
			// } else if float64(counterMediumPriority)/float64(counter+1) < mediumPriorityRatio {
			// 	priority = model.MEDIUM_PRIORITY
			// 	counterMediumPriority++
			// }
			// counter++

			parallelismSemaphore.Acquire()
			wg.Add(1)

            go func(deadline time.Time, bitrate model.Bitrate) {
                defer func() {
                    parallelismSemaphore.Release()
                    wg.Done()
                }()

			remaining := time.Until(deadline)
			if remaining <= 0 {
				fmt.Printf("Skipped (timeout) segment %d, tile %d\n", segment, tile)
				ratio, complete := agg.Record(segment, tile, false)
				tmrValue := -1.0
				if complete {
					tmrValue = ratio
				}
				if statisticsLogger != nil {
					bufferSec := playbackSimulator.GetBufferLevel(int(lastDownloadedSegment.Load())).Seconds()
					statisticsLogger.Log(time.Since(startTime), model.VideoPacketRequest{
						ID:       uuid.Nil,
						Priority: priority,
						Bitrate:  bitrate,
						Segment:  segment,
						Tile:     tile,
						Timeout:  0,
					}, 0, true, true, false, 0.0, bufferSec, tmrValue)
				}
				return
			}
				timeoutMs := int(remaining / time.Millisecond)
				if timeoutMs <= 0 {
					timeoutMs = 1
				}
				var instaThroughput float64 // Declara instaThroughput aqui para ter o escopo correto

				request := model.VideoPacketRequest{
					ID:       uuid.Must(uuid.New(), nil),
					Priority: priority,
					Bitrate:  bitrate,
					Segment:  segment,
					Tile:     tile,
					Timeout:  timeoutMs,
				}

                // Log de envio da requisição
                fmt.Printf("Sending request for segment %d, tile %d with priority %d\n", segment, tile, priority)

                // Marca o primeiro request de mídia (Join latency: t0)
                firstRequestOnce.Do(func() { firstRequestTime = time.Now() })

                // Registra o tempo de envio da requisição ANTES de enviá-la
                sendBufferSec := playbackSimulator.GetBufferLevel(int(lastDownloadedSegment.Load())).Seconds()
                collector.RecordSend(request.ID)

				// As linhas abaixo foram removidas pois o cálculo de vazão era prematuro e com dados errados
				// requestBytes, err := json.Marshal(request)
				// if err != nil {
				// 	return
				// }
				// sizeInBytes := len(requestBytes)
				// _, instaThroughput := collector.RecordRecv(request.ID, sizeInBytes)

				requestTime := time.Since(startTime)
				response := client.Request(request, remaining)
				responseTime := time.Since(startTime)

				// A chamada para collector.RecordSend foi movida para antes do request.
				// Esta linha original é agora redundante e deve ser removida/comentada.
				// collector.RecordSend(request.ID)

				var timedOut bool
				var late bool
				if response == nil {
					fmt.Printf("Timeout: no response for segment %d, tile %d\n", segment, tile)
					timedOut = true
					instaThroughput = 0.0 // Define como 0.0 para caso de timeout
				} else {
					if len(response.Data) == 0 {
						log.Panicf("Empty response for (%d, %d)", segment, tile)
					}
					// Registra o recebimento da resposta com o tamanho correto dos dados.
					bytesReceived := len(response.Data)
					_, instaThroughput = collector.RecordRecv(request.ID, bytesReceived) // Atribui ao instaThroughput já declarado

					late = time.Now().After(deadline)
					staleAgg.Add(bytesReceived, late)

					if late {
						fmt.Printf("Late response for segment %d, tile %d\n", segment, tile)
						timedOut = true
					} else {
						fmt.Printf("Received response for segment %d, tile %d\n", segment, tile)
						timedOut = false
					}
				}

			// Record on-time arrival for aggregator (responseTime <= deadline)
			onTime := (response != nil) && (!timedOut)
			ratio, complete := agg.Record(segment, tile, onTime)
			tmrValue := -1.0
			if complete {
				tmrValue = ratio
			}

				if response != nil {
					// Atualiza o último segmento baixado de forma atômica, apenas se o novo segmento for maior
					for {
						oldValue := lastDownloadedSegment.Load()
						if int32(segment) > oldValue {
							if lastDownloadedSegment.CompareAndSwap(oldValue, int32(segment)) {
								break // Atualizado com sucesso
							}
							// Se CompareAndSwap falhou, outro goroutine atualizou, tenta novamente
						} else {
							break // Segmento atual não é maior, não precisa atualizar
						}
					}
				}

			if statisticsLogger != nil {
				statisticsLogger.Log(requestTime, request,
					responseTime-requestTime, timedOut, false, !timedOut, instaThroughput, sendBufferSec, tmrValue)
			}
			}(segmentDeadline, segmentBitrate)
		}
	}

	    log.Println("Waiting for all goroutines to finish...")
	    wg.Wait()
	    log.Println("All goroutines completed.")
	    fmt.Println("Test iteration complete.")

	    // Cálculo e registro de Join latency e Segment completion rate
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

        // Compute segment completion rate (ALL tiles) in percentage
        completionRate := agg.Rate(firstSegment, lastSegment)
        log.Printf("Segment completion rate (ALL tiles): %.2f%%", completionRate)

        staleRatio := staleAgg.RatioPercent()
        log.Printf("Stale bytes ratio: %.2f%%", staleRatio)

        if summaryLogger != nil {
            summaryLogger.LogSession(joinLatency, completionRate, staleRatio)
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
