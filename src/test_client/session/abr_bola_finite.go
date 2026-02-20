package session

import (
	"log"
	"math"

	"main/src/model"
)

// bolaFiniteABR implementa BOLA-Finite adaptado para streaming 360 tiled.
// A decisao eh por segmento: escolhe uma configuracao (bitrate FOV e nao-FOV).
type bolaFiniteABR struct {
	sizeProvider TileSizeProvider
	qmaxSegments float64
	gamma        float64
	wFOV         float64
	wNonFOV      float64
	configs      []SegmentConfig
}

// bolaConfigOption agrega custo e utilidade de uma configuracao candidata.
type bolaConfigOption struct {
	cfg     SegmentConfig
	size    int64
	utility float64
}

// TileSizeProvider fornece uma estimativa de tamanho medio do tile (em LOW).
type TileSizeProvider interface {
	AvgSize(tileID int) int64
}

// NewBOLAFiniteABR cria o BOLA-Finite com defaults e tamanhos do dataset.
func NewBOLAFiniteABR() ABRController {
	estimator, err := NewTileSizeEstimator("data/segments")
	if err != nil {
		log.Printf("BOLA ABR: failed to build tile size estimator: %v (using fallback)", err)
		estimator = NewFallbackTileSizeEstimator()
	}
	return newBolaFiniteABRWithEstimator(estimator)
}

func newBolaFiniteABRWithEstimator(estimator TileSizeProvider) ABRController {
	return &bolaFiniteABR{
		sizeProvider: estimator,
		qmaxSegments: 3,   // max de buffer em segmentos
		gamma:        1.0, // peso do termo de smoothness do BOLA
		wFOV:         1.0, // peso de utilidade para tiles em FOV
		wNonFOV:      0.2, // peso de utilidade para tiles fora do FOV
		configs: []SegmentConfig{
			{ID: "A_all_low", FOVBitrate: model.LOW_BITRATE, NonFOVBitrate: model.LOW_BITRATE},
			{ID: "B_fov_med", FOVBitrate: model.MEDIUM_BITRATE, NonFOVBitrate: model.LOW_BITRATE},
			{ID: "C_fov_high", FOVBitrate: model.HIGH_BITRATE, NonFOVBitrate: model.LOW_BITRATE},
		},
	}
}

func (b *bolaFiniteABR) SelectConfig(ctx SegmentContext) SegmentConfig {
	// 1) Validacoes basicas e parametros de tempo.
	if len(b.configs) == 0 {
		return SegmentConfig{ID: "A_all_low", FOVBitrate: model.LOW_BITRATE, NonFOVBitrate: model.LOW_BITRATE}
	}

	p := ctx.SegmentDuration.Seconds()
	if p <= 0 {
		return b.configs[0]
	}

	// 2) Conjunto FOV para separar tiles em FOV vs nao-FOV.
	fovSet := make(map[int]struct{}, len(ctx.FOVTiles))
	for _, t := range ctx.FOVTiles {
		fovSet[t] = struct{}{}
	}

	fovCount := 0
	nonFOVCount := 0
	var fovBaseSize int64
	var nonFOVBaseSize int64

	// 3) Soma dos tamanhos medios (em LOW) para FOV e nao-FOV.
	for _, tileID := range ctx.AllTiles {
		size := b.sizeProvider.AvgSize(tileID)
		if size <= 0 {
			size = 1
		}
		if _, ok := fovSet[tileID]; ok {
			fovCount++
			fovBaseSize += size
		} else {
			nonFOVCount++
			nonFOVBaseSize += size
		}
	}

	if fovCount+nonFOVCount == 0 {
		return b.configs[0]
	}

	// 4) Para cada configuracao, calcula custo (bytes) e utilidade total.
	options := make([]bolaConfigOption, 0, len(b.configs))
	for _, cfg := range b.configs {
		size := scaleSize(fovBaseSize, cfg.FOVBitrate) + scaleSize(nonFOVBaseSize, cfg.NonFOVBitrate)
		utility := b.wFOV*float64(fovCount)*utilityFor(cfg.FOVBitrate) +
			b.wNonFOV*float64(nonFOVCount)*utilityFor(cfg.NonFOVBitrate)
		options = append(options, bolaConfigOption{
			cfg:     cfg,
			size:    size,
			utility: utility,
		})
	}

	// 5) Encontra utilidade maxima para normalizar o BOLA-Finite.
	uMax := options[0].utility
	for i := 1; i < len(options); i++ {
		if options[i].utility > uMax {
			uMax = options[i].utility
		}
	}
	if uMax <= 0 {
		return b.configs[0]
	}

	// 6) Parametros do BOLA-Finite (buffer em segmentos e horizonte finito).
	Q := ctx.BufferLevel.Seconds() / p
	remainingSeg := ctx.LastSegment - ctx.SegmentID + 1
	if remainingSeg < 1 {
		remainingSeg = 1
	}
	remainingSec := float64(remainingSeg) * p
	playbackPos := float64(ctx.SegmentID-ctx.FirstSegment) * p
	if playbackPos < 0 {
		playbackPos = 0
	}

	t := math.Min(playbackPos, remainingSec)
	tPrime := math.Max(t/2.0, 3.0*p)
	QDmax := math.Min(b.qmaxSegments, tPrime/p)

	denom := uMax + b.gamma*p
	if denom <= 0 {
		denom = 1e-9
	}
	V := (QDmax - 1.0) / denom

	// 7) Score BOLA: escolhe a configuracao com melhor score.
	bestIdx := 0
	bestScore := math.Inf(-1)
	for i, opt := range options {
		if opt.size <= 0 {
			continue
		}
		score := (V*(opt.utility+b.gamma*p) - Q) / float64(opt.size)
		if score > bestScore {
			bestScore = score
			bestIdx = i
		}
	}
	if bestScore <= 0 {
		bestIdx = 0
	}

	chosen := options[bestIdx]
	chosenScore := bestScore

	// 8) Guardrail por throughput: derruba config se exceder o budget do segmento.
	guardrail := false
	if ctx.AvgThroughput > 0 && ctx.TimeBudget > 0 {
		budgetBytes := ctx.AvgThroughput * ctx.TimeBudget.Seconds()
		if budgetBytes > 0 && float64(chosen.size) > budgetBytes {
			for i := bestIdx; i >= 0; i-- {
				if options[i].size > 0 && float64(options[i].size) <= budgetBytes {
					chosen = options[i]
					guardrail = true
					break
				}
			}
			if float64(options[0].size) > budgetBytes {
				chosen = options[0]
				guardrail = true
			}
		}
	}

	// 9) Logs de depuracao para validar o BOLA-Finite adaptado 360 tiled.
	throughputKbps := ctx.AvgThroughput / 1000.0
	budgetKB := (ctx.AvgThroughput * ctx.TimeBudget.Seconds()) / 1024.0
	chosenKB := float64(chosen.size) / 1024.0
	log.Printf("ABR (BOLA): seg=%d cfg=%s fov=%s nonfov=%s buf=%.2fs thr=%.1f kbps budget=%.1f KB size=%.1f KB score=%.4f guardrail=%t uMax=%.2f V=%.4f Q=%.2f",
		ctx.SegmentID, chosen.cfg.ID, chosen.cfg.FOVBitrate, chosen.cfg.NonFOVBitrate,
		Q*p, throughputKbps, budgetKB, chosenKB, chosenScore, guardrail, uMax, V, Q)

	return chosen.cfg
}

func utilityFor(bitrate model.Bitrate) float64 {
	// Utilidade concava para evitar saltos agressivos.
	return math.Log(float64(bitrate))
}

func scaleSize(base int64, bitrate model.Bitrate) int64 {
	// Escala linear do tamanho medio em LOW para outros bitrates.
	if base <= 0 {
		return 0
	}
	return int64(float64(base) * float64(bitrate) / float64(model.LOW_BITRATE))
}
