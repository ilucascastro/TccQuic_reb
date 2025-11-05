package metrics

import (
	"encoding/csv"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type WFQUtil struct {
	mu      sync.Mutex
	file    *os.File
	w       *csv.Writer
	ticker  *time.Ticker
	stop    chan struct{}
	classes []ClassInt

	weights map[ClassInt]float64 // pesos alvo normalizados
	bytes   map[ClassInt]int64   // bytes por janela
}

var wfqOnce sync.Once
var wfqInst *WFQUtil

func StartWFQUtilWriter(csvPath string, classes []ClassInt, interval time.Duration) {
	wfqOnce.Do(func() {
		if csvPath == "" {
			csvPath = "/tmp/server_scheduler_test/wfq_utilization.csv"
		}
		_ = os.MkdirAll(filepath.Dir(csvPath), 0o755)
		f, _ := os.OpenFile(csvPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		w := csv.NewWriter(f)
		if st, _ := f.Stat(); st != nil && st.Size() == 0 {
			_ = w.Write([]string{
				"ts", "w_low", "w_medium", "w_high",
				"share_low", "share_medium", "share_high",
				"err_low", "err_medium", "err_high", "mae",
			})
			w.Flush()
		}
		wfqInst = &WFQUtil{
			file:    f,
			w:       w,
			ticker:  time.NewTicker(interval),
			stop:    make(chan struct{}),
			classes: classes,
			weights: map[ClassInt]float64{},
			bytes:   map[ClassInt]int64{},
		}
		go wfqInst.loop()
	})
}

func StopWFQUtilWriter() {
	if wfqInst == nil {
		return
	}
	close(wfqInst.stop)
	wfqInst.ticker.Stop()
	wfqInst.w.Flush()
	_ = wfqInst.file.Close()
	wfqInst = nil
}

// defina os pesos WFQ (ex.: {low:1, medium:2, high:3})
func SetWFQWeights(weights map[ClassInt]float64) {
	if wfqInst == nil {
		return
	}
	// normaliza p/ somar 1
	var sum float64
	for _, c := range wfqInst.classes {
		sum += weights[c]
	}
	wfqInst.mu.Lock()
	wfqInst.weights = map[ClassInt]float64{}
	if sum > 0 {
		for _, c := range wfqInst.classes {
			wfqInst.weights[c] = weights[c] / sum
		}
	}
	wfqInst.mu.Unlock()
}

// chame quando uma requisição COMPLETA enviar bytes>0
func RecordBytesForWFQ(class ClassInt, bytes int) {
	if wfqInst == nil || bytes <= 0 {
		return
	}
	wfqInst.mu.Lock()
	wfqInst.bytes[class] += int64(bytes)
	wfqInst.mu.Unlock()
}

func (u *WFQUtil) loop() {
	for {
		select {
		case <-u.stop:
			return
		case now := <-u.ticker.C:
			u.mu.Lock()
			var total int64
			for _, c := range u.classes {
				total += u.bytes[c]
			}
			obs := map[ClassInt]float64{}
			for _, c := range u.classes {
				if total > 0 {
					obs[c] = float64(u.bytes[c]) / float64(total)
				} else {
					obs[c] = 0
				}
			}
			err := map[ClassInt]float64{}
			var mae float64
			for _, c := range u.classes {
				e := math.Abs(obs[c] - u.weights[c])
				err[c] = e
				mae += e
			}
			mae /= float64(len(u.classes))

			rec := []string{
				now.Format(time.RFC3339Nano),
				f614(u.weights[0]), f614(u.weights[1]), f614(u.weights[2]),
				f614(obs[0]), f614(obs[1]), f614(obs[2]),
				f614(err[0]), f614(err[1]), f614(err[2]),
				f614(mae),
			}
			_ = u.w.Write(rec)
			u.w.Flush()
			for k := range u.bytes {
				u.bytes[k] = 0
			}
			u.mu.Unlock()
		}
	}
}

func f614(v float64) string { return strconv.FormatFloat(v, 'f', 6, 64) }