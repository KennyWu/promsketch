package promsketch

import (
	"flag"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shirou/gopsutil/v3/mem"

	"github.com/stretchr/testify/require"
	"github.com/zzylol/prometheus-sketch-VLDB/prometheus-sketches/model/labels"
)

func TestNewSketchCacheInstance(t *testing.T) {
	lset := labels.FromStrings("fake_metric", "machine0")
	ps := NewPromSketches()
	require.NotEmpty(t, ps)

	err := ps.NewSketchCacheInstance(lset, "avg_over_time", 100000, 100000, 10000)
	require.NoError(t, err)
	err = ps.NewSketchCacheInstance(lset, "quantile_over_time", 100000, 100000, 10000)
	require.NoError(t, err)
	err = ps.NewSketchCacheInstance(lset, "distinct_over_time", 1000000, 10000, 10000)
	require.NoError(t, err)
}

func TestLookUpWithQuantileQuery(t *testing.T) {
	lset := labels.FromStrings("fake_metric", "machine0")
	ps := NewPromSketches()
	require.NotEmpty(t, ps)

	lookup := ps.LookUp(lset, "quantile_over_time", 0, 10)
	require.Equal(t, false, lookup)

	err := ps.NewSketchCacheInstance(lset, "quantile_over_time", 100000, 100000, 10000)
	require.NoError(t, err)

	for time := 0; time < 20; time++ {
		err := ps.SketchInsert(lset, int64(time), 0.5+float64(time))
		require.NoError(t, err)
	}

	lookup = ps.LookUp(lset, "quantile_over_time", 0, 10)
	require.Equal(t, true, lookup)
}

func TestLookUpWithSumQuery(t *testing.T) {
	lset := labels.FromStrings("fake_metric", "machine0")
	ps := NewPromSketches()
	require.NotEmpty(t, ps)

	lookup := ps.LookUp(lset, "sum_over_time", 0, 10)
	require.Equal(t, false, lookup)

	err := ps.NewSketchCacheInstance(lset, "sum_over_time", 100000, 100000, 10000)
	require.NoError(t, err)

	for time := 0; time < 20; time++ {
		err := ps.SketchInsert(lset, int64(time), 0.5+float64(time))
		require.NoError(t, err)
	}

	lookup = ps.LookUp(lset, "sum_over_time", 0, 10)
	require.Equal(t, true, lookup)
}

func TestEvalQuantile(t *testing.T) {
	lset := labels.FromStrings("fake_metric", "machine0")
	ps := NewPromSketches()
	require.NotEmpty(t, ps)

	err := ps.NewSketchCacheInstance(lset, "quantile_over_time", 100000, 100000, 10000)
	require.NoError(t, err)

	for time := 0; time < 20; time++ {
		err := ps.SketchInsert(lset, int64(time), 0.5+float64(time))
		require.NoError(t, err)
	}
	lookup := ps.LookUp(lset, "quantile_over_time", 0, 10)
	require.Equal(t, true, lookup)

	vector, _ := ps.Eval("quantile_over_time", lset, 0.6, 1, 10, 10)
	require.Equal(t, vector, Vector{Sample{F: 6.5, T: 0}})
}

/*
func BenchmarkSketchInsertDefinedRules(b *testing.B) {
	sc := &SketchConfig{
		EH_univ_config:  EHUnivConfig{K: 20, Time_window_size: 1000000},
		EH_kll_config:   EHKLLConfig{K: 100, Kll_k: 256, Time_window_size: 1000000},
		Sampling_config: SamplingConfig{Sampling_rate: 0.05, Time_window_size: 1000000, Max_size: 50000},
	}

	lset := labels.FromStrings("fake_metric", "machine0")

	avgsmap := make(map[SketchType]bool)
	avgsmap[EHCount] = true
	entropysmap := make(map[SketchType]bool)
	entropysmap[SHUniv] = true
	entropysmap[EffSum] = true
	entropysmap[EHCount] = true
	quantilesmap := make(map[SketchType]bool)
	quantilesmap[EHDD] = true
	ruletest := []SketchRuleTest{
		{"avg_over_time", funcAvgOverTime, lset, -1, 1000000, 1000000, avgsmap},
		{"count_over_time", funcCountOverTime, lset, -1, 1000000, 1000000, avgsmap},
		{"entropy_over_time", funcEntropyOverTime, lset, -1, 1000000, 1000000, entropysmap},
		{"l1_over_time", funcL1OverTime, lset, -1, 1000000, 1000000, entropysmap},
		{"quantile_over_time", funcQuantileOverTime, lset, 0.5, 1000000, 1000000, quantilesmap},
	}

	ps := NewPromSketchesWithConfig(ruletest, sc)

	for n := 0; n < b.N; n++ {
		t := int64(time.Now().UnixMicro())
		value := float64(0)
		for {
			value = rand.NormFloat64() + 5000
			if value >= 0 && value <= 10000 {
				break
			}
		}
		err := ps.SketchInsertDefinedRules(lset, t, value)
		if err != nil {
			fmt.Println("sketch insert error")
			return
		}
	}
}
*/

var flagvar int

const timeDelta = 100

func init() {
	flag.IntVar(&flagvar, "numts", 1000, "number of timeseries")
}
func TestInsertThroughput(t *testing.T) {
	scrapeCountBatch := 2160000 // 60 hours
	num_ts := flagvar
	promcache := NewPromSketches()

	lsets := make([]labels.Labels, 0)
	for j := 0; j < num_ts; j++ {
		fakeMetric := "machine" + strconv.Itoa(j)
		// inputLabel := labels.FromStrings("fake_metric", fakeMetric, fakeMetric1, fakeMetric2, fakeMetric3, fakeMetric4, fakeMetric5, fakeMetric6, fakeMetric7, fakeMetric8, fakeMetric9, fakeMetric10, fakeMetric11, fakeMetric12, fakeMetric13, fakeMetric14, fakeMetric15, fakeMetric16, fakeMetric17, fakeMetric18, fakeMetric19, fakeMetric20)
		inputLabel := labels.FromStrings("fake_metric", fakeMetric)
		lsets = append(lsets, inputLabel)
		// promcache.NewSketchCacheInstance(inputLabel, "sum_over_time", 100000000, 1000000, 10000)
		// promcache.NewSketchCacheInstance(inputLabel, "quantile_over_time", 100000000, 1000000, 10000)
		promcache.NewSketchCacheInstance(inputLabel, "entropy_over_time", 1000000, 10000, 10000)
	}

	start := time.Now()
	ingestScrapesUniform(lsets, scrapeCountBatch, promcache)

	since := time.Since(start)

	throughput := float64(scrapeCountBatch) * float64(num_ts) / float64(since.Seconds())
	t.Log(num_ts, since.Seconds(), throughput)

}

func ingestScrapesUniform(lbls []labels.Labels, scrapeCount int, promcache *PromSketches) (uint64, error) {
	var total atomic.Uint64
	ts_per_worker := int(len(lbls) / 64)
	if len(lbls)%64 != 0 {
		ts_per_worker += 1
	}
	if ts_per_worker > 100 {
		ts_per_worker = 100
	}
	scrapeCountBatch := 100
	for i := 0; i < scrapeCount; i += scrapeCountBatch {
		var wg sync.WaitGroup
		lbls := lbls
		for len(lbls) > 0 {
			l := 100
			if len(lbls) < 100 {
				l = len(lbls)
			}
			batch := lbls[:l]
			lbls = lbls[l:]

			wg.Add(1)
			go func() {
				defer wg.Done()

				// fmt.Println(i)
				// promcache.PrintEHUniv(batch[0])

				ts := int64(timeDelta * i)

				var ato_total atomic.Uint64

				for i := 0; i < scrapeCountBatch; i++ {
					ts += timeDelta

					for j := 0; j < len(batch); j += 1 {
						err := promcache.SketchInsert(batch[j], ts, float64(rand.Float64()*100000))
						if err != nil {
							panic(err)
						}
						ato_total.Add(1)
					}
				}

				total.Add(ato_total.Load())
			}()
		}
		wg.Wait()
	}
	fmt.Println("ingestion completed")

	v, _ := mem.VirtualMemory()

	// almost every return value is a struct
	fmt.Printf("Total: %v, Free:%v, UsedPercent:%f%%\n", v.Total, v.Free, v.UsedPercent)

	// convert to JSON. String() is also implemented
	fmt.Println(v)
	return total.Load(), nil
}
