package session

import (
	"fmt"
	"os"
)

// TileSizeEstimator computes average tile sizes from the dataset.
// Sizes are interpreted as LOW bitrate and scaled linearly for higher bitrates.
type TileSizeEstimator struct {
	byTile    map[int]int64
	globalAvg int64
}

func NewTileSizeEstimator(dir string) (*TileSizeEstimator, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	sums := make(map[int]int64)
	counts := make(map[int]int64)
	var totalSize int64
	var totalCount int64

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		var tileID int
		var segmentID int
		if _, err := fmt.Sscanf(name, "video_tiled_10_dash_track%d_%d.m4s", &tileID, &segmentID); err != nil {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		size := info.Size()
		sums[tileID] += size
		counts[tileID]++
		totalSize += size
		totalCount++
	}

	byTile := make(map[int]int64, len(sums))
	for tileID, sum := range sums {
		count := counts[tileID]
		if count > 0 {
			byTile[tileID] = sum / count
		}
	}

	globalAvg := int64(0)
	if totalCount > 0 {
		globalAvg = totalSize / totalCount
	}
	if globalAvg <= 0 {
		globalAvg = 1
	}

	return &TileSizeEstimator{
		byTile:    byTile,
		globalAvg: globalAvg,
	}, nil
}

func NewFallbackTileSizeEstimator() *TileSizeEstimator {
	return &TileSizeEstimator{
		byTile:    map[int]int64{},
		globalAvg: 1,
	}
}

func (t *TileSizeEstimator) AvgSize(tileID int) int64 {
	if t == nil {
		return 0
	}
	if v, ok := t.byTile[tileID]; ok && v > 0 {
		return v
	}
	return t.globalAvg
}
