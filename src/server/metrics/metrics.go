package metrics

import (
	"encoding/csv"
	"log"
	"main/src/model"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type Class = model.Priority

// -------- contadores por classe --------

type classCounters struct {
	Enqueued, Started, Completed, DroppedDeadline  int64
	BytesSent, BytesOnTime                         int64
	QueueDelaySum, ServiceTimeSum, ResponseTimeSum int64 // ms

	SlackSum      int64 // ms
	TimeToDropSum int64 // ms
	OnTimeCount   int64 // reqs concluídas no prazo
}

// -------- métricas globais --------
type globalCounters struct {
	// Concurrency de serviço
	inService int64

	// Preempção e inversão
	Preemptions int64
	Inversions  int64

	// Work-conserving
	lastTick                  time.Time
	queuePositiveDur          time.Duration // tempo com Q>0
	idleWhileQueuePositiveDur time.Duration // tempo com Q>0 e inService==0

	// Stale bytes (bytes que teriam sido enviados mas expiraram)
	StaleBytes int64
}

// -------- CSV writers --------

type csvOut struct {
	f  *os.File
	w  *csv.Writer
	mu sync.Mutex
}

func (c *csvOut) open(path string, hdr []string) {
	if path == "" {
		return
	}
	_ = os.MkdirAll(filepath.Dir(path), 0o755)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		log.Printf("[METRICS] csv open %s: %v", path, err)
		return
	}
	w := csv.NewWriter(f)
	if st, _ := f.Stat(); st != nil && st.Size() == 0 {
		_ = w.Write(hdr)
		w.Flush()
	}
	c.f, c.w = f, w
}

func (c *csvOut) write(row []string) {
	if c == nil || c.w == nil {
		return
	}
	c.mu.Lock()
	_ = c.w.Write(row)
	c.w.Flush()
	c.mu.Unlock()
}

func (c *csvOut) close() {
	if c == nil {
		return
	}
	c.mu.Lock()
	if c.w != nil {
		c.w.Flush()
	}
	if c.f != nil {
		_ = c.f.Close()
	}
	c.mu.Unlock()
}

// -------- Metrics singleton --------

type Metrics struct {
	mu  sync.Mutex
	cls map[Class]*classCounters
	gl  globalCounters

	// queue len (por classe) mais recente
	queueLen map[Class]int

	// CSV paths
	classAggPath string
	queueLenPath string
	summaryPath  string

	// CSV writers
	classAgg csvOut
	queueCSV csvOut // <- RENOMEADO (antes era queueLen)

	// também mantemos um writer para o summary
	summary csvOut

	// run timing
	runStart time.Time
}

type TaskCtx struct {
	Class      Class
	EnqueuedAt time.Time
	Deadline   time.Time
	StartedAt  time.Time
}

var (
	globalInst *Metrics
	once       sync.Once
)

func M() *Metrics {
	once.Do(func() {
		m := &Metrics{
			cls:      map[Class]*classCounters{},
			queueLen: map[Class]int{},
		}
		for c := Class(0); c < Class(model.PRIORITY_LEVEL_COUNT); c++ {
			m.cls[c] = &classCounters{}
			m.queueLen[c] = 0
		}
		m.gl.lastTick = time.Now()
		globalInst = m
	})
	return globalInst
}

// -------------------- Init & CSVs --------------------

func (m *Metrics) InitClassAgg(path string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.classAggPath = path
	m.classAgg.open(path, []string{
		"ts",
		"class",
		"event", // complete | drop
		"completed",
		"dropped_deadline",
		"bytes_sent",
		"bytes_on_time",
		"avg_queue_delay_ms",
		"avg_service_time_ms",
		"avg_response_time_ms",
		"ontime_ratio_pct",
		"bytes_on_time_ratio_pct",
		"avg_slack_ms",
		"avg_time_to_drop_ms",
	})
}

func (m *Metrics) InitQueueCSV(path string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queueLenPath = path
	m.queueCSV.open(path, []string{"ts", "class", "queue_len"}) // <- usa queueCSV
}

func (m *Metrics) InitSummary(path string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.summaryPath = path
	m.summary.open(path, []string{
		"ts_start", "ts_end", "duration_s",
		"bytes_low", "bytes_med", "bytes_high",
		"throughput_low_kbps", "throughput_med_kbps", "throughput_high_kbps",
		"class_share_low_pct", "class_share_med_pct", "class_share_high_pct",
		"jain_fairness",
		"drop_rate_low_pct", "drop_rate_med_pct", "drop_rate_high_pct",
		"preemptions", "inversions",
		"work_conserving_ratio_pct",
		"stale_bytes",
	})
}

func (m *Metrics) MarkRunStart() {
	m.mu.Lock()
	m.runStart = time.Now()
	m.mu.Unlock()
}

// -------------------- Internals --------------------

func (m *Metrics) toClassName(c Class) string {
	switch c {
	case model.LOW_PRIORITY:
		return "low"
	case model.MEDIUM_PRIORITY:
		return "medium"
	case model.HIGH_PRIORITY:
		return "high"
	default:
		return "unknown"
	}
}

func i64(v int64) string   { return strconv.FormatInt(v, 10) }
func f64(v float64) string { return strconv.FormatFloat(v, 'f', 3, 64) }

func div(sum int64, n int64) float64 {
	if n == 0 {
		return 0
	}
	return float64(sum) / float64(n)
}
func ratioPct(num int64, den int64) float64 {
	if den == 0 {
		return 0
	}
	return 100.0 * float64(num) / float64(den)
}

func (m *Metrics) writeClassAggRow(className, event string, cl *classCounters) {
	m.classAgg.write([]string{
		time.Now().Format(time.RFC3339Nano),
		className,
		event,
		i64(cl.Completed),
		i64(cl.DroppedDeadline),
		i64(cl.BytesSent),
		i64(cl.BytesOnTime),
		f64(div(cl.QueueDelaySum, cl.Started)),
		f64(div(cl.ServiceTimeSum, cl.Completed)),
		f64(div(cl.ResponseTimeSum, cl.Completed)),
		f64(ratioPct(cl.OnTimeCount, cl.Completed)),
		f64(ratioPct(cl.BytesOnTime, cl.BytesSent)),
		f64(div(cl.SlackSum, cl.Started)),
		f64(div(cl.TimeToDropSum, cl.DroppedDeadline)),
	})
}

// Atualiza contabilidade de work-conserving com base no estado atual.
func (m *Metrics) updateWorkConservingLocked(now time.Time) {
	// total queued
	qtot := 0
	for _, q := range m.queueLen {
		qtot += q
	}
	// tempo desde última amostra
	dt := now.Sub(m.gl.lastTick)
	if dt < 0 {
		dt = 0
	}
	if qtot > 0 {
		m.gl.queuePositiveDur += dt
		if m.gl.inService == 0 {
			m.gl.idleWhileQueuePositiveDur += dt
		}
	}
	m.gl.lastTick = now
}

// -------------------- Eventos básicos --------------------

func (m *Metrics) OnEnqueue(class Class) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// apenas contador — fila real deve ser amostrada via OnQueueSample
	m.cls[class].Enqueued++
	// atualizar work-conserving relógio
	m.updateWorkConservingLocked(time.Now())
}

func (m *Metrics) OnStart(ctx *TaskCtx) {
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	cl := m.cls[ctx.Class]
	cl.Started++
	cl.QueueDelaySum += now.Sub(ctx.EnqueuedAt).Milliseconds()

	// slack = deadline - agora (>=0)
	slack := ctx.Deadline.Sub(now).Milliseconds()
	if slack < 0 {
		slack = 0
	}
	cl.SlackSum += slack

	// Concurrency
	m.gl.inService++

	// Inversão de ordem: se há alguma fila com classe superior > 0
	// e estamos iniciando uma classe inferior, conta inversão.
	// Prioridade: low=0 < med=1 < high=2
	for c, q := range m.queueLen {
		if int(c) > int(ctx.Class) && q > 0 {
			m.gl.Inversions++
			break
		}
	}

	m.updateWorkConservingLocked(now)
	ctx.StartedAt = now
}

func (m *Metrics) OnComplete(ctx *TaskCtx, bytes int, dropped bool) {
	now := time.Now()

	m.mu.Lock()
	cl := m.cls[ctx.Class]
	cl.Completed++
	cl.ServiceTimeSum += now.Sub(ctx.StartedAt).Milliseconds()
	cl.ResponseTimeSum += now.Sub(ctx.EnqueuedAt).Milliseconds()
	cl.BytesSent += int64(bytes)
	if !dropped && (now.Before(ctx.Deadline) || now.Equal(ctx.Deadline)) {
		cl.BytesOnTime += int64(bytes)
		cl.OnTimeCount++
	}
	// Concurrency down
	if m.gl.inService > 0 {
		m.gl.inService--
	}
	className := m.toClassName(ctx.Class)
	m.updateWorkConservingLocked(now)
	m.mu.Unlock()

	// escreve 1 linha por COMPLETE (snapshot por classe)
	m.mu.Lock()
	m.writeClassAggRow(className, "complete", cl)
	m.mu.Unlock()
}

// OnDeadlineDropWithBytes registra drop por deadline e bytes estagnados estimados.
func (m *Metrics) OnDeadlineDropWithBytes(ctx *TaskCtx, estBytes int64) {
	now := time.Now()

	m.mu.Lock()
	cl := m.cls[ctx.Class]
	cl.DroppedDeadline++
	cl.TimeToDropSum += now.Sub(ctx.EnqueuedAt).Milliseconds()
	m.gl.StaleBytes += estBytes

	// Concurrency down (se algum serviço estivesse ativo para esse ctx; em geral inicia com 0)
	if m.gl.inService > 0 {
		m.gl.inService--
	}
	className := m.toClassName(ctx.Class)
	m.updateWorkConservingLocked(now)
	m.mu.Unlock()

	// escreve 1 linha por DROP (snapshot por classe)
	m.mu.Lock()
	m.writeClassAggRow(className, "drop", cl)
	m.mu.Unlock()
}

// Compatibilidade: versão sem bytes estimados (soma 0).
func (m *Metrics) OnDeadlineDrop(ctx *TaskCtx) {
	m.OnDeadlineDropWithBytes(ctx, 0)
}

// Preempção explícita (se a política implementar).
func (m *Metrics) OnPreempt(preempted, preemptor Class) {
	m.mu.Lock()
	m.gl.Preemptions++
	m.updateWorkConservingLocked(time.Now())
	m.mu.Unlock()
}

// Amostra tamanhos de fila por classe; útil para queue_len e work-conserving.
func (m *Metrics) OnQueueSample(lens map[Class]int) {
	now := time.Now()
	m.mu.Lock()
	for c, v := range lens {
		m.queueLen[c] = v
		// escreve por classe
		m.queueCSV.write([]string{ // <- usa queueCSV
			now.Format(time.RFC3339Nano),
			m.toClassName(c),
			strconv.Itoa(v),
		})
	}
	m.updateWorkConservingLocked(now)
	m.mu.Unlock()
}

// -------------------- Summary --------------------

func (m *Metrics) WriteSummaryAndClose() {
	m.mu.Lock()
	defer m.mu.Unlock()

	tsStart := m.runStart
	tsEnd := time.Now()
	dur := tsEnd.Sub(tsStart).Seconds()
	if dur <= 0 {
		dur = 1
	}

	// per-class bytes & rates
	bl := float64(m.cls[model.LOW_PRIORITY].BytesSent)
	bm := float64(m.cls[model.MEDIUM_PRIORITY].BytesSent)
	bh := float64(m.cls[model.HIGH_PRIORITY].BytesSent)
	bt := bl + bm + bh
	shL, shM, shH := 0.0, 0.0, 0.0
	if bt > 0 {
		shL = 100.0 * bl / bt
		shM = 100.0 * bm / bt
		shH = 100.0 * bh / bt
	}
	// Jain fairness sobre shares normalizados (somando 1.0)
	xL, xM, xH := 0.0, 0.0, 0.0
	if bt > 0 {
		xL = bl / bt
		xM = bm / bt
		xH = bh / bt
	}
	sum := xL + xM + xH
	sum2 := xL*xL + xM*xM + xH*xH
	jain := 0.0
	if sum2 > 0 {
		jain = (sum * sum) / (3.0 * sum2)
	}

	// Throughput kbps
	tL := (bl * 8.0 / 1000.0) / dur
	tM := (bm * 8.0 / 1000.0) / dur
	tH := (bh * 8.0 / 1000.0) / dur

	// Drop rate por classe (sobre enqueued)
	dr := func(c Class) float64 {
		enq := m.cls[c].Enqueued
		if enq == 0 {
			return 0
		}
		return 100.0 * float64(m.cls[c].DroppedDeadline) / float64(enq)
	}

	// Work-conserving ratio (porcentagem do tempo com Q>0 em que ficamos ociosos)
	wcr := 0.0
	if m.gl.queuePositiveDur > 0 {
		wcr = 100.0 * float64(m.gl.idleWhileQueuePositiveDur) / float64(m.gl.queuePositiveDur)
	}

	m.summary.write([]string{
		tsStart.Format(time.RFC3339Nano),
		tsEnd.Format(time.RFC3339Nano),
		f64(dur),
		i64(int64(m.cls[model.LOW_PRIORITY].BytesSent)),
		i64(int64(m.cls[model.MEDIUM_PRIORITY].BytesSent)),
		i64(int64(m.cls[model.HIGH_PRIORITY].BytesSent)),
		f64(tL), f64(tM), f64(tH),
		f64(shL), f64(shM), f64(shH),
		f64(jain),
		f64(dr(model.LOW_PRIORITY)), f64(dr(model.MEDIUM_PRIORITY)), f64(dr(model.HIGH_PRIORITY)),
		i64(m.gl.Preemptions), i64(m.gl.Inversions),
		f64(wcr),
		i64(m.gl.StaleBytes),
	})

	// fechar CSVs
	m.classAgg.close()
	m.queueCSV.close() // <- usa queueCSV
	m.summary.close()
}