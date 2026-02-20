package stream_handler

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"main/src/model"
	"main/src/server/metrics"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/lucas-clemente/quic-go"
)

//
// ============================== VISÃO GERAL ===============================
//
// Este handler recebe streams QUIC do cliente e processa requisições de
// “tiles” de vídeo (segment/tile) através de um TaskScheduler com política
// (FIFO/SP/WFQ).
//
// CSVs gerados (lado servidor):
// 1) reqlog.csv        — por requisição (tempos e status por request)
// 2) class_agg.csv     — agregado por classe (médias e somatórios por classe)
// 3) queue_len.csv     — (opcional) amostras de tamanho de fila por classe (se o scheduler expor)
// 4) server_summary.csv — resumo ao final (shares, Jain, throughput, contadores)
//
// ========================================================================
// ===== CSV SIMPLES EMBUTIDO (sem dependências externas) =====

type csvSink struct {
	mu     sync.Mutex
	f      *os.File
	w      *csv.Writer
	header []string
	path   string
}

// newCSVSink cria (e se vazio, escreve header) um writer de CSV em "path".
func newCSVSink(path string, header []string) *csvSink {
	dir := filepath.Dir(path)
	_ = os.MkdirAll(dir, 0o755)

	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		log.Printf("[CSV] open %s: %v", path, err)
		return nil
	}
	w := csv.NewWriter(f)
	// header se novo (arquivo zerado)
	if st, _ := f.Stat(); st != nil && st.Size() == 0 {
		_ = w.Write(header)
		w.Flush()
	}
	log.Printf("[CSV] ready at %s", path)
	return &csvSink{f: f, w: w, header: header, path: path}
}

// write adiciona uma linha no CSV com flush imediato (durabilidade).
func (s *csvSink) write(rec []string) {
	if s == nil || s.w == nil {
		return
	}
	s.mu.Lock()
	_ = s.w.Write(rec)
	s.w.Flush()
	s.mu.Unlock()
}

// close garante flush e fecha o arquivo.
func (s *csvSink) close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.w != nil {
		s.w.Flush()
	}
	if s.f != nil {
		_ = s.f.Close()
	}
	s.mu.Unlock()
}

// remoteDir é o diretório remoto onde guardamos logs/CSVs no host Mininet.
const remoteDir = "/tmp/server_scheduler_test"

// Interface opcional: se o TaskScheduler implementar, amostramos filas periodicamente.
type QueueLenProvider interface {
	// Retorna tamanho das filas por classe (prioridade → len).
	QueueLenPerClass() map[model.Priority]int
}

// StreamHandler orquestra o loop de leitura de streams e o escalonamento.
type StreamHandler struct {
	taskScheduler TaskScheduler

	reqlog       *csvSink // CSV por requisição
	queueSampler *time.Ticker
	stopSample   chan struct{}
}

// NewStreamHandler instancia o handler com a política desejada.
func NewStreamHandler(policy QueuePolicy) *StreamHandler {
	return &StreamHandler{
		taskScheduler: NewTaskScheduler(policy),
	}
}

// Start inicializa o scheduler e os CSVs.
// Também fixa o caminho do CSV de agregados por classe via metrics.M().
func (s *StreamHandler) Start() {
	// garanta logs no stdout
	log.SetOutput(os.Stdout)
	log.Println("[SERVER] StreamHandler starting")

	// 1) REQLOG (por requisição) — usado para CDF/p95 etc.
	s.reqlog = newCSVSink(
		filepath.Join(remoteDir, "reqlog.csv"),
		[]string{
			"time_ns", "event", "class", "segment", "tile",
			"bytes", "ontime", "drop",
			"qd_ms", "svc_ms", "rsp_ms",
		},
	)

	// 2) AGREGADOS POR CLASSE (módulo metrics) — arquivo separado
	metrics.M().InitClassAgg(filepath.Join(remoteDir, "class_agg.csv"))

	// 3) Queue length CSV & work-conserving (opcional via provider)
	metrics.M().InitQueueCSV(filepath.Join(remoteDir, "queue_len.csv"))
	metrics.M().InitSummary(filepath.Join(remoteDir, "server_summary.csv"))
	metrics.M().MarkRunStart()

	// Se o scheduler expõe QueueLenPerClass, amostrar periodicamente
	if qp, ok := any(s.taskScheduler).(QueueLenProvider); ok {
		s.stopSample = make(chan struct{})
		s.queueSampler = time.NewTicker(100 * time.Millisecond)
		go func() {
			for {
				select {
				case <-s.queueSampler.C:
					m := qp.QueueLenPerClass()
					metrics.M().OnQueueSample(m)
				case <-s.stopSample:
					return
				}
			}
		}()
	} else {
		log.Println("[SERVER] QueueLenProvider not implemented by scheduler (queue_len.csv disabled)")
	}

	// run scheduler (loop de escalonamento)
	go s.taskScheduler.Run()

	// fairness (1s)
	metrics.StartFairnessWriter(filepath.Join(remoteDir, "fairness.csv"), []int{0, 1, 2}, 1*time.Second)

	// work-conserving (1s)
	metrics.StartWorkConservingWriter(filepath.Join(remoteDir, "work_conserving.csv"), 1*time.Second)

	// wfq utilization (1s) — só fará sentido se a política for WFQ; pode setar peso padrão
	metrics.StartWFQUtilWriter(filepath.Join(remoteDir, "wfq_utilization.csv"), []int{0, 1, 2}, 1*time.Second)

	log.Println("[SERVER] StreamHandler started")
}

// Stop encerra o scheduler, sampling e fecha CSVs.
func (s *StreamHandler) Stop() {
	log.Println("[SERVER] stopping...")
	s.taskScheduler.Stop()

	if s.queueSampler != nil {
		s.queueSampler.Stop()
	}
	if s.stopSample != nil {
		close(s.stopSample)
	}

	// Resumo final (shares, Jain, throughput, contadores)
	metrics.M().WriteSummaryAndClose()

	if s.reqlog != nil {
		s.reqlog.close()
	}
	log.Println("[SERVER] stopped")

	metrics.StopFairnessWriter()
	metrics.StopWorkConservingWriter()
	metrics.StopWFQUtilWriter()

}

// HandleStream é chamado para cada novo stream QUIC aceito.
func (s *StreamHandler) HandleStream(quicStream quic.Stream) {
	log.Printf("[STREAM] accepted id=%d", quicStream.StreamID())

	go (&stream{
		parent:        s,
		taskScheduler: s.taskScheduler,
		quicStream:    quicStream,
		reader:        bufio.NewReader(quicStream),
		writer:        bufio.NewWriter(quicStream),
		usageCount:    0,
	}).listen()
}

// stream encapsula o ciclo de vida de uma conexão de pedidos no QUIC.
type stream struct {
	parent        *StreamHandler
	taskScheduler TaskScheduler
	quicStream    quic.Stream
	reader        *bufio.Reader
	writer        *bufio.Writer
	usageCount    int
}

// decreaseUsageCount fecha o stream quando o uso chega a zero.
func (s *stream) decreaseUsageCount() {
	s.usageCount--
	if s.usageCount == 0 {
		_ = s.quicStream.Close()
		log.Printf("[STREAM] closed id=%d", s.quicStream.StreamID())
	}
}

// listen lê requisições do cliente, agenda execução e registra métricas.
// Fluxo da métrica por request:
//
//	ENQUEUE  -> metrics.M().OnEnqueue(class)
//	START    -> metrics.M().OnStart(ctx)
//	COMPLETE -> metrics.M().OnComplete(ctx, bytes, dropped=false)   OU
//	DROP     -> metrics.M().OnDeadlineDropWithBytes(ctx, estBytes)
func (s *stream) listen() {
	s.usageCount++
	defer s.decreaseUsageCount()

	for {
		// 1) Leitura do pedido (segment/tile/priority/timeout)
		req, err := model.ReadVideoPacketRequest(s.reader)
		if req == nil {
			if err != nil && err != io.EOF {
				log.Printf("[REQ] read error: %v", err)
			} else {
				log.Printf("[STREAM] eof id=%d", s.quicStream.StreamID())
			}
			return
		}
		log.Printf("[REQ] recv seg=%d tile=%d prio=%d timeout_ms=%d",
			req.Segment, req.Tile, req.Priority, req.Timeout)

		// 2) Marcação de chegada + deadline
		enqueuedAt := time.Now()
		deadline := enqueuedAt.Add(time.Duration(req.Timeout) * time.Millisecond)

		// 3) MÉTRICAS (agregados): evento de fila
		metrics.M().OnEnqueue(req.Priority)

		// 4) Contexto da tarefa p/ correlacionar tempos e classe
		ctx := &metrics.TaskCtx{
			Class:      req.Priority,
			EnqueuedAt: enqueuedAt,
			Deadline:   deadline,
		}

		// 5) Enfileirar no escalonador conforme a política
		s.usageCount++
		ok := s.taskScheduler.Enqueue(req.Priority, func() {
			defer s.decreaseUsageCount()

			// 5.1) START (marca início de serviço e computa slack/inversão)
			metrics.M().OnStart(ctx)

			// 5.2) Métricas de tempos por request (reqlog)
			startedAt := time.Now()
			qdMs := startedAt.Sub(enqueuedAt).Milliseconds()

			// 5.3) Serviço: lê arquivo e envia resposta no QUIC
			bytes := s.handleRequestMeasured(req, deadline)
			if bytes > 0 {
				metrics.RecordBytesForFairness(int(req.Priority), bytes)
				metrics.RecordBytesForWFQ(int(req.Priority), bytes)
			}

			now := time.Now()
			svcMs := now.Sub(startedAt).Milliseconds()
			rspMs := now.Sub(enqueuedAt).Milliseconds()
			onTime := bytes > 0 && (now.Before(deadline) || now.Equal(deadline))
			deadlineDrop := bytes <= 0 && now.After(deadline)

			// 5.4) MÉTRICAS (agregados): COMPLETE vs DROP por deadline
			if deadlineDrop {
				// estimar "stale bytes" usando tamanho do arquivo (se existir)
				est := int64(estimateTileSize(req))
				metrics.M().OnDeadlineDropWithBytes(ctx, est)
			} else {
				metrics.M().OnComplete(ctx, bytes /*dropped=*/, false)
			}

			// 5.5) REQLOG (linha por request) — tempos e flags
			if s.parent != nil && s.parent.reqlog != nil {
				s.parent.reqlog.write([]string{
					fmt.Sprintf("%d", now.UnixNano()),
					func() string {
						if deadlineDrop {
							return "drop"
						}
						return "complete"
					}(),
					fmt.Sprintf("%d", req.Priority),
					fmt.Sprintf("%d", req.Segment),
					fmt.Sprintf("%d", req.Tile),
					fmt.Sprintf("%d", bytes),
					fmt.Sprintf("%t", onTime),
					fmt.Sprintf("%t", deadlineDrop),
					fmt.Sprintf("%d", qdMs),
					fmt.Sprintf("%d", svcMs),
					fmt.Sprintf("%d", rspMs),
				})
			}

			log.Printf(
				"[METRICS_REQ] seg=%d tile=%d prio=%d bytes=%d ontime=%t drop=%t qd_ms=%d svc_ms=%d rsp_ms=%d",
				req.Segment, req.Tile, req.Priority, bytes, onTime, deadlineDrop, qdMs, svcMs, rspMs,
			)
		})
		if !ok {
			log.Println("[SCHED] task enqueue failed")
			return
		}
	}
}

// handleRequestMeasured executa o “serviço”: valida deadline,
// carrega o tile do disco e envia a resposta via QUIC.
// Retorna o número de bytes efetivamente enviados (0 em falha/timeout).
func (s *stream) handleRequestMeasured(req *model.VideoPacketRequest, deadline time.Time) int {
	// Se já passou o deadline, não vale mais processar (drop por deadline).
	if time.Now().After(deadline) {
		log.Printf("[REQ] timed out before service seg=%d tile=%d", req.Segment, req.Tile)
		return 0
	}

	data := readFile(req)
	if data == nil || len(data) == 0 {
		// Falha de E/S não conta como deadline drop — bytes=0 e ontime=false
		log.Printf("[REQ] file empty/missing seg=%d tile=%d", req.Segment, req.Tile)
		return 0
	}

	res := model.VideoPacketResponse{
		Priority: req.Priority,
		Bitrate:  req.Bitrate,
		Segment:  req.Segment,
		Tile:     req.Tile,
		Data:     data,
	}
	if err := res.Write(s.writer); err != nil {
		log.Printf("[RESP] write error: %v", err)
		return 0
	}
	// flush é essencial para não acumular no buffer e atrasar deadline
	if err := s.writer.Flush(); err != nil {
		log.Printf("[RESP] flush error: %v", err)
		return 0
	}

	log.Printf("[RESP] sent seg=%d tile=%d bytes=%d", req.Segment, req.Tile, len(data))
	return len(data)
}

// readFile monta o caminho do arquivo do tile e lê do disco.
func readFile(req *model.VideoPacketRequest) []byte {
	basePath, err := os.Getwd()
	if err != nil {
		log.Printf("[FS] getwd err: %v", err)
	}
	// dataset naming is track{tile}_{segment}, so tile first, segment second
	filePath := fmt.Sprintf("/data/segments/video_tiled_10_dash_track%d_%d.m4s",
		req.Tile, req.Segment)
	full := basePath + filePath
	data, err := os.ReadFile(full)
	if err != nil {
		log.Printf("[FS] read err: %v path=%s", err, full)
		return nil
	}
	return data
}

// estimateTileSize retorna tamanho do arquivo do tile (ou 0 se faltante).
func estimateTileSize(req *model.VideoPacketRequest) int64 {
	basePath, _ := os.Getwd()
	full := fmt.Sprintf("%s/data/segments/video_tiled_10_dash_track%d_%d.m4s",
		basePath, req.Tile, req.Segment)
	st, err := os.Stat(full)
	if err != nil {
		return 0
	}
	return st.Size()
}
