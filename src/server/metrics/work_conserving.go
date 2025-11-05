package metrics

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type WorkConserving struct {
	mu     sync.Mutex
	file   *os.File
	w      *csv.Writer
	ticker *time.Ticker
	stop   chan struct{}
	last   time.Time

	backlog   int  // total de itens enfileirados (todas as classes)
	inService bool // true enquanto alguma tarefa está sendo processada

	winBusyNs    int64 // backlog>0 && inService
	winIdleNs    int64 // backlog>0 && !inService
	winBacklogNs int64 // backlog>0
}

var wcOnce sync.Once
var wcInst *WorkConserving

func StartWorkConservingWriter(csvPath string, interval time.Duration) {
	wcOnce.Do(func() {
		if csvPath == "" {
			csvPath = "/tmp/server_scheduler_test/work_conserving.csv"
		}
		_ = os.MkdirAll(filepath.Dir(csvPath), 0o755)
		f, _ := os.OpenFile(csvPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		w := csv.NewWriter(f)
		if st, _ := f.Stat(); st != nil && st.Size() == 0 {
			_ = w.Write([]string{"ts", "busy_ms", "idle_ms", "backlog_ms", "ratio"})
			w.Flush()
		}
		wcInst = &WorkConserving{
			file:   f,
			w:      w,
			ticker: time.NewTicker(interval),
			stop:   make(chan struct{}),
			last:   time.Now(),
		}
		go wcInst.loop()
	})
}

func StopWorkConservingWriter() {
	if wcInst == nil {
		return
	}
	close(wcInst.stop)
	wcInst.ticker.Stop()
	wcInst.w.Flush()
	_ = wcInst.file.Close()
	wcInst = nil
}

// chame quando o total de itens enfileirados mudar
func UpdateBacklog(total int) {
	if wcInst == nil {
		return
	}
	now := time.Now()
	wcInst.mu.Lock()
	wcInst.tickLocked(now)
	wcInst.backlog = total
	wcInst.mu.Unlock()
}

// chame quando iniciar/parar processamento
func UpdateServiceState(running bool) {
	if wcInst == nil {
		return
	}
	now := time.Now()
	wcInst.mu.Lock()
	wcInst.tickLocked(now)
	wcInst.inService = running
	wcInst.mu.Unlock()
}

func (w *WorkConserving) loop() {
	for {
		select {
		case <-w.stop:
			return
		case now := <-w.ticker.C:
			w.mu.Lock()
			w.tickLocked(now)
			w.flushLocked(now)
			w.mu.Unlock()
		}
	}
}

// acumula tempo desde w.last conforme estado atual
func (w *WorkConserving) tickLocked(now time.Time) {
	dt := now.Sub(w.last)
	w.last = now
	if w.backlog > 0 {
		w.winBacklogNs += dt.Nanoseconds()
		if w.inService {
			w.winBusyNs += dt.Nanoseconds()
		} else {
			w.winIdleNs += dt.Nanoseconds()
		}
	}
}

func (w *WorkConserving) flushLocked(now time.Time) {
	var ratio float64 = 1.0
	if w.winBacklogNs > 0 {
		ratio = float64(w.winBusyNs) / float64(w.winBusyNs+w.winIdleNs)
	}
	rec := []string{
		now.Format(time.RFC3339Nano),
		ms(w.winBusyNs), ms(w.winIdleNs), ms(w.winBacklogNs),
		f64(ratio),
	}
	_ = w.w.Write(rec)
	w.w.Flush()
	w.winBusyNs, w.winIdleNs, w.winBacklogNs = 0, 0, 0
}

func ms(ns int64) string { return strconv.FormatInt(ns/1e6, 10) }