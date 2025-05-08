package promsketch

import (
	"flag"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

//func funcMadOverTime(vals []parser.Value, args parser.Expressions, enh *EvalNodeHelper) (Vector, annotations.Annotations) {
//	if len(vals[0].(Matrix)[0].Floats) == 0 {
//		return enh.Out, nil
//	}
//	return aggrOverTime(vals, enh, func(s Series) float64 {
//		values := make(vectorByValueHeap, 0, len(s.Floats))
//		for _, f := range s.Floats {
//			values = append(values, Sample{F: f.F})
//		}
//		median := quantile(0.5, values)
//		values = make(vectorByValueHeap, 0, len(s.Floats))
//		for _, f := range s.Floats {
//			values = append(values, Sample{F: math.Abs(f.F - median)})
//		}
//		return quantile(0.5, values)
//	}), nil
//}

var coreSketchRerunAttempts = *flag.Int("coreRerunAttempts", 3, "Number of attempts to rerun core sketch")
var coreBucketSize = *flag.Int("coreBucketSize", 80000, "Number of buckets per core")
var concurrent = *flag.Bool("concurrent", true, "Whether to run concurrent core sketches")
var threads = *flag.Int("threads", 4, "Number of threads to run")
var minVal = 0.01
var maxVal = 100.0
var src = rand.New(rand.NewSource(time.Now().UnixNano()))
var s = 1.01
var v = 1.0
var zipf = rand.NewZipf(src, s, v, uint64(maxVal*10.0))

var TestCases = []struct{ Datasize int }{
	{
		10,
	},
	{
		100,
	},
	{
		400,
	},
	{
		600,
	},
	{
		934,
	},
	{
		1000,
	},
	{
		10000,
	},
	{
		100000,
	},
	{
		1000000,
	},
	{
		10000000,
	},
	{
		100000000,
	},
}

func runTest(t *testing.T, dataSize int, data []float64) {
	for i := 0; i < coreSketchRerunAttempts; i++ {
		var coreMad float64
		now := time.Now()
		if concurrent {
			coreMad = CoreMadConcurrent(data, maxVal, minVal, coreBucketSize, threads)
		} else {
			coreMad = CoreMadMain(data, maxVal, minVal, coreBucketSize)
		}

		coreDelta := time.Since(now).Seconds()
		now = time.Now()
		mad := exactMad(data)
		exactDelta := time.Since(now).Seconds()
		if concurrent {
			fmt.Printf("[%d] CORE_MAD_CONCURRENT=%f, EXACT_MAD=%f\n", dataSize, coreMad, mad)
			fmt.Printf("[%d] CORE_MAD_CONCURRENT_TIME(S):%f\n", dataSize, coreDelta)
		} else {
			fmt.Printf("[%d] CORE_MAD=%f, EXACT_MAD=%f\n", dataSize, coreMad, mad)
			fmt.Printf("[%d] CORE_MAD_TIME(S):%f\n", dataSize, coreDelta)
		}
		fmt.Printf("[%d] EXACT_MAD_TIME(S):%f\n", dataSize, exactDelta)
	}
}

func TestCoreSketchZipf(t *testing.T) {
	for _, testCase := range TestCases {
		data := make([]float64, testCase.Datasize)
		for i := 0; i < testCase.Datasize; i++ {
			data[i] = GenerateZipfFloat()
		}

		runTest(t, testCase.Datasize, data)

	}
}

func TestCoreSketchBimodal(t *testing.T) {
	for _, testCase := range TestCases {
		data := make([]float64, testCase.Datasize)
		for i := 0; i < testCase.Datasize; i++ {
			data[i] = generateBimodal(30, 5, 60, 5)
		}

		runTest(t, testCase.Datasize, data)
	}
}

func TestCoreSketchTailDist(t *testing.T) {
	for _, testCase := range TestCases {
		data := make([]float64, testCase.Datasize)
		for i := 0; i < testCase.Datasize; i++ {
			data[i] = generateTailDist(50, 10, 55)
		}

		runTest(t, testCase.Datasize, data)
	}
}

func TestCoreSketchNormal(t *testing.T) {
	for _, testCase := range TestCases {
		data := make([]float64, testCase.Datasize)
		for i := 0; i < testCase.Datasize; i++ {
			data[i] = generateNormal(50, 5)
		}

		runTest(t, testCase.Datasize, data)
	}
}

func generateTailDist(mean, stddev, minThreshold float64) float64 {
	for {
		val := rand.NormFloat64()*stddev + mean

		if val > minThreshold {
			return val
		}
	}

}

func generateBimodal(mean1, stdev1, mean2, stdev2 float64) float64 {
	if rand.Float64() < 0.5 {
		return generateNormal(mean1, stdev1)
	} else {
		return generateNormal(mean2, stdev2)
	}
}

func GenerateZipfFloat() float64 {
	n := zipf.Uint64() + 1 // shift to [1, imax + 1]

	return float64(n) * (float64(maxVal) / float64((maxVal*10)+1))
}

func generateNormal(mean, stddev float64) float64 {
	val := rand.NormFloat64()*stddev + mean
	// Clamp to [0, 1]
	if val < 0 {
		return 0
	} else if val > maxVal {
		return maxVal
	}
	return val
}
