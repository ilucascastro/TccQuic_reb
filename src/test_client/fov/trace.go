package fov

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// O FOVTrace armazena o mapeamento entre segmentos de mídia (tempo) e os tiles que
// estavam dentro do campo de visão do usuário durante esse período.
type FOVTrace struct {
	framesPerSegment int
	tilesBySegment   map[int]map[int]struct{}
	maxSegment       int
}

// LoadFOVTrace analisa um rastreamento CSV com o seguinte formato:
// nº de quadros, números de blocos
// 00001, 49, 50, ...
//
// A primeira coluna é o número do quadro (indexado a partir de 1), as colunas restantes são
// identificadores de tiles vistos dentro do campo de visão. O carregador agrupa os quadros de acordo com
// fps e duração do segmento para obter a lista de blocos por segmento de mídia.
func LoadFOVTrace(path string, fps int, segmentDuration time.Duration) (*FOVTrace, error) {
	if fps <= 0 {
		return nil, fmt.Errorf("invalid fps=%d", fps)
	}
	if segmentDuration <= 0 {
		return nil, fmt.Errorf("invalid segmentDuration=%s", segmentDuration)
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	framesPerSegment := int(segmentDuration.Seconds()*float64(fps) + 0.5)
	if framesPerSegment <= 0 {
		return nil, fmt.Errorf("segmentDuration=%s and fps=%d yield framesPerSegment=%d", segmentDuration, fps, framesPerSegment)
	}

	trace := &FOVTrace{
		framesPerSegment: framesPerSegment,
		tilesBySegment:   make(map[int]map[int]struct{}),
	}

	scanner := bufio.NewScanner(file)
	lineNo := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		lineNo++

		// Skip header if present
		if lineNo == 1 {
			if strings.Contains(line, "frames") {
				continue
			}
		}
		if len(line) == 0 {
			continue
		}

		fields := strings.Split(line, ",")
		if len(fields) < 2 {
			continue
		}

		frameStr := strings.TrimSpace(fields[0])
		frameIdx, err := strconv.Atoi(frameStr)
		if err != nil {
			return nil, fmt.Errorf("line %d: invalid frame number %q: %w", lineNo, frameStr, err)
		}
		if frameIdx <= 0 {
			continue
		}
		segmentIdx := (frameIdx-1)/framesPerSegment + 1
		if segmentIdx <= 0 {
			continue
		}

		segmentSet := trace.tilesBySegment[segmentIdx]
		if segmentSet == nil {
			segmentSet = make(map[int]struct{})
			trace.tilesBySegment[segmentIdx] = segmentSet
		}

		for _, token := range fields[1:] {
			val := strings.TrimSpace(token)
			if val == "" {
				continue
			}
			tileID, err := strconv.Atoi(val)
			if err != nil {
				return nil, fmt.Errorf("line %d: invalid tile number %q: %w", lineNo, val, err)
			}
			if tileID <= 0 {
				continue
			}
			segmentSet[tileID] = struct{}{}
		}

		if segmentIdx > trace.maxSegment {
			trace.maxSegment = segmentIdx
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return trace, nil
}

// TilesForSegment retorna uma slice com os tiles que estavam dentro do campo de visão (FOV) para
// o índice do segmento de mídia fornecido. A slice é uma cópia e pode ser modificada por
// quem a chamou.
func (t *FOVTrace) TilesForSegment(segment int) []int {
	if t == nil || segment <= 0 {
		return nil
	}
	set := t.tilesBySegment[segment]
	if len(set) == 0 {
		return nil
	}
	result := make([]int, 0, len(set))
	for tile := range set {
		result = append(result, tile)
	}
	return result
}

// Contém informações sobre se o tile fornecido estava dentro do campo de visão (FOV) para o índice de segmento de mídia fornecido.
//
func (t *FOVTrace) Contains(segment int, tile int) bool {
	if t == nil || segment <= 0 {
		return false
	}
	set := t.tilesBySegment[segment]
	if set == nil {
		return false
	}
	_, ok := set[tile]
	return ok
}

// MaxSegment returns the largest media-segment index present in the trace.
func (t *FOVTrace) MaxSegment() int {
	if t == nil {
		return 0
	}
	return t.maxSegment
}
