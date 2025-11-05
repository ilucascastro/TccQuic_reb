package metrics

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type ClassInt = int

type Fairness struct {
	mu       sync.Mutex
	file     *os.File
	w        *csv.Writer
	ticker   *time.Ticker
	stop     chan struct{}
	classes  []ClassInt
	bytesWin map[ClassInt]int64
}

var fairnessOnce sync.Once
var fairnessInst *Fairness

func StartFairnessWriter(csvPath string, classes []ClassInt, interval time.Duration) {
	fairnessOnce.Do(func() {
		if csvPath == "" {
			csvPath = "/tmp/server_scheduler_test/fairness.csv"
		}
		_ = os.MkdirAll(filepath.Dir(csvPath), 0o755)
		f, _ := os.OpenFile(csvPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		w := csv.NewWriter(f)
		if st, _ := f.Stat(); st != nil && st.Size() == 0 {
			_ = w.Write([]string{"ts", "bytes_low", "bytes_medium", "bytes_high",
				"share_low", "share_medium", "share_high", "jain"})
			w.Flush()
		}
		fairnessInst = &Fairness{
			file:     f,
			w:        w,
			ticker:   time.NewTicker(interval),
			stop:     make(chan struct{}),
			classes:  classes,
			bytesWin: map[ClassInt]int64{},
		}
		go fairnessInst.loop()
	})
}

func StopFairnessWriter() {
	if fairnessInst == nil {
		return
	}
	close(fairnessInst.stop)
	fairnessInst.ticker.Stop()
	fairnessInst.w.Flush()
	_ = fairnessInst.file.Close()
	fairnessInst = nil
}

// chame isso quando uma requisição COMPLETA enviar bytes>0
func RecordBytesForFairness(class ClassInt, bytes int) {
	if fairnessInst == nil || bytes <= 0 {
		return
	}
	fairnessInst.mu.Lock()
	fairnessInst.bytesWin[class] += int64(bytes)
	fairnessInst.mu.Unlock()
}

func (f *Fairness) loop() {
	for {
		select {
		case <-f.stop:
			return
		case now := <-f.ticker.C:
			f.mu.Lock()
			var total int64
			for _, c := range f.classes {
				total += f.bytesWin[c]
			}
			share := map[ClassInt]float64{}
			var s, s2 float64
			for _, c := range f.classes {
				if total > 0 {
					share[c] = float64(f.bytesWin[c]) / float64(total)
				} else {
					share[c] = 0
				}
				s += share[c]
				s2 += share[c] * share[c]
			}
			var jain float64
			n := float64(len(f.classes))
			if s2 > 0 {
				jain = (s * s) / (n * s2)
			}
			rec := []string{
				now.Format(time.RFC3339Nano),
				i642(f.bytesWin[0]), i642(f.bytesWin[1]), i642(f.bytesWin[2]),
				f642(share[0]), f642(share[1]), f642(share[2]), f642(jain),
			}
			_ = f.w.Write(rec)
			f.w.Flush()
			for k := range f.bytesWin {
				f.bytesWin[k] = 0
			}
			f.mu.Unlock()
		}
	}
}

func i642(v int64) string   { return strconv.FormatInt(v, 10) }
func f642(v float64) string { return strconv.FormatFloat(v, 'f', 6, 64) }