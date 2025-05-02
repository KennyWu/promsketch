package promsketch

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"
)

const DATA_SIZE int = 10000000

func BenchMarkCoreSketch(b *testing.B) {
	data := make([]float64, DATA_SIZE)
	for i := 0; i < DATA_SIZE; i++ {
		//data[i] = boundedNormal(50, 10)
		data[i] = rand.Float64() + 0.01
	}

}

func TestCoreSketch(t *testing.T) {
	data := make([]float64, DATA_SIZE)
	for i := 0; i < DATA_SIZE; i++ {
		data[i] = boundedNormal(50, 10)
		//data[i] = rand.Float64() + 0.01
	}
	madCorrect := exactMad(data, len(data))
	madReallyCorrect := mad(data)
	madSketch := CoreMadMain(data, 1, 0.01, 20000)
	fmt.Printf("Correct: %f, NonquickSelect: %f, madSketch: %f", madCorrect, madReallyCorrect, madSketch)
	fmt.Printf("Sketch: %f", madSketch)
}

func median(data []float64) float64 {
	n := len(data)
	if n == 0 {
		return math.NaN()
	}
	sorted := append([]float64{}, data...)
	sort.Float64s(sorted)
	mid := n / 2
	if n%2 == 0 {
		return (sorted[mid-1] + sorted[mid]) / 2
	}
	return sorted[mid]
}

func boundedNormal(mean, stddev float64) float64 {
	val := rand.NormFloat64()*stddev + mean
	// Clamp to [0, 1]
	if val < 0 {
		return 0
	} else if val > 100 {
		return 100
	}
	return val
}

// mad computes the Median Absolute Deviation of a slice of float64s
func mad(data []float64) float64 {
	if len(data) == 0 {
		return math.NaN()
	}
	med := median(data)
	deviations := make([]float64, len(data))
	for i, v := range data {
		deviations[i] = math.Abs(v - med)
	}
	return median(deviations)
}

func exactMad(data []float64, len int) float64 {
	queue := make([]float64, len, len)
	for i, val := range data {
		queue[i] = val
	}

	median := getKth(queue, 0, len, (len-1)/2)
	fmt.Printf("Median: %f ", median)
	for i, val := range queue {
		queue[i] = AbsFloat64(val - median)
	}

	mad := getKth(queue, 0, len, (len-1)/2)

	return mad
}
