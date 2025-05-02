package promsketch

import (
	"math"
	"math/rand"
	"sort"
)

const SPACE_LIMIT = 1024

var LOG_2 = math.Log(2)

type CoreSketch struct {
	power_2_card, frac__power_2_card, card__log_2 float64
	l, r, m                                       int
	card                                          int
	maxBucket                                     int
	usefulRange                                   []float64
	buckets                                       map[int64]int64
	data_size                                     int64
	pruned_size                                   int64
	// Count from left to median bucket
	left_count int64
	// Count from median to right bucket
	right_count     int64
	mid_num         []int64
	bucket_sequence []coreBucket
}

type coreBucket struct {
	index       int64
	count       int64
	lower_bound float64
	upper_bound float64
	// Not needed for floats, uncomment for exclusively int data
	// finest bool
}

/*
Corresponds to CORE_MAD.java here
https://github.com/thssdb/core-sketch/blob/main/code/src/main/java/benchmark/CORE_MAD.java
===================================================================================================
*/
// ONLY WORKS WITH POSITIVE NON ZERO VALUES
func calculateCard(maxValue float64, minValue float64, spaceLimit int) int {
	card := 0
	for card <= 30 {
		spaceExpected := math.Ceil(math.Pow(2, float64(card))*math.Log(maxValue)/math.Log(2)) -
			math.Floor(math.Pow(2, float64(card))*math.Log(minValue)/math.Log(2))

		if spaceExpected > float64(spaceLimit) {
			return card - 1
		}
		card++
	}
	return card
}

func calculateCardRange(realRange []float64, spaceLimit int) int {
	card := 0
	for card <= 30 {
		spaceExpected := 0
		for i := 0; i < int(realRange[6]); i++ {
			space := math.Ceil(math.Pow(2, float64(card))*math.Log(realRange[2*i+1])/math.Log(2)) -
				math.Floor(math.Pow(2, float64(card))*math.Log(realRange[2*i])/math.Log(2))

			spaceExpected += int(space)
		}

		if spaceExpected > spaceLimit {
			return card - 1
		}
		card++
	}
	return card
}

func FinestSketch(data []float64, max_val float64, min_val float64, space_limit int, des int, rang []float64) *CoreSketch {
	//threadMemory := 0.0
	card := 0
	if rang[0] == min_val && rang[1] == max_val {
		card = calculateCard(max_val, min_val, space_limit)
	} else {
		card = calculateCardRange(rang, space_limit)
	}
	sketch := NewCoreSketchWithParams(card, space_limit, rang)
	for true {
		pre_card := sketch.card
		for i, _ := range data {
			sketch.Insert(data[i])
		}

		// TODO: measure memory
		m := sketch.MidHalfCountBucket()
		lr := sketch.EdgeHalfCountBucket(m)

		next_range := sketch.GenerateUsefulRange(m, lr[0], lr[1])
		pre_sketch := sketch
		sketch = NewCoreSketchWithParams(0, space_limit, next_range)
		real_range := sketch.RealRange()
		card = calculateCardRange(real_range, space_limit)

		if card == pre_card {
			sketch.SetCard(card)
			return pre_sketch
		}
		sketch.SetCard(card)
	}

	return nil
}

func calculateMad(data []float64, sketch *CoreSketch) float64 {
	usefulRange := sketch.GetRange()
	sketch = NewCoreSketch()
	sketch.SetRange(usefulRange)
	queue := make([]float64, 0)

	for _, datum := range data {
		sketch.InsertMid(datum)
		if sketch.InRange(datum) {
			queue = append(queue, datum)
		}
	}
	if len(queue) == 0 {
		return math.NaN()
	}

	midNum := sketch.GetGap()
	queueN := len(queue)
	var medianRank int = (len(data)-1)/2 - int(midNum[1]) - int(midNum[0])
	if medianRank < 0 {
		medianRank += int(midNum[1])
	}
	median := getKth(queue, 0, queueN, medianRank)
	for i, val := range queue {
		queue[i] = math.Abs(val - median)
	}
	madRank := int((len(data)-1)/2) - int(midNum[1]) - int(midNum[2])
	if madRank < 0 {
		madRank += int(midNum[1]) + int(midNum[2])
	}
	mad := getKth(queue, 0, queueN, madRank)
	return mad
}

func getKth(data []float64, L, R, K int) float64 {
	// Choose random pivot
	pos := L + rand.Intn(R-L)
	pivotV := data[pos]

	// Move pivot to the end of equal region
	R-- // Since we use 0-based and R is exclusive
	data[pos], data[R] = data[R], pivotV

	leP := L // End of "< pivotV" region
	eqR := R // Start of "== pivotV" region

	for i := L; i < eqR; i++ {
		swapV := data[i]
		if swapV < pivotV {
			data[i], data[leP] = data[leP], swapV
			leP++
		} else if swapV == pivotV {
			eqR--
			data[i], data[eqR] = data[eqR], swapV
			i-- // Stay on same index to reprocess swapped-in value
		}
	}

	// Move all == pivotV to middle (between < and >)
	countEqual := R - eqR + 1
	for i := 0; i < countEqual; i++ {
		data[leP+i], data[eqR+i] = data[eqR+i], data[leP+i]
	}

	// Recursive logic
	if K < leP-L {
		return getKth(data, L, leP, K)
	}
	if K >= leP-L+countEqual {
		return getKth(data, leP+countEqual, R+1, K-(leP-L)-countEqual)
	}
	return pivotV
}

// this is called core_mad_original in the original java code
func CoreMadMain(data []float64, max_val float64, min_val float64, space_limit int) float64 {
	rang := []float64{min_val, max_val, min_val, max_val, min_val, max_val, 1}
	sketch := FinestSketch(data, max_val, min_val, space_limit, len(data), rang)
	return calculateMad(data, sketch)
}

/*
Corresponds to CORESketch.java here
https://github.com/thssdb/core-sketch/blob/main/code/src/main/java/mad/CORESketch.java
===================================================================================================
*/

func newCoreBucket(index int64, count int64, lower_bound float64, upper_bound float64) *coreBucket {
	b := &coreBucket{}
	b.index = index
	b.count = count
	b.lower_bound = lower_bound
	b.upper_bound = upper_bound

	return b
}

// TODO: delete if unused?
func NewCoreSketch() *CoreSketch {
	s := &CoreSketch{}

	s.card = 0
	s.power_2_card = math.Pow(2, float64(s.card))
	s.frac__power_2_card = s.power_2_card / LOG_2
	s.maxBucket = SPACE_LIMIT
	s.usefulRange = make([]float64, 6)
	s.buckets = make(map[int64]int64, s.maxBucket)
	s.mid_num = make([]int64, 4)
	s.bucket_sequence = make([]coreBucket, s.maxBucket)
	s.left_count = 0
	s.right_count = 0
	s.m = 0
	s.l = 0
	s.r = 0

	return s
}

func NewCoreSketchWithParams(card int, maxBucket int, usefulRange []float64) *CoreSketch {
	s := &CoreSketch{}

	s.card = card
	s.power_2_card = math.Pow(2, float64(card))
	s.frac__power_2_card = s.power_2_card / LOG_2
	s.maxBucket = maxBucket
	s.buckets = make(map[int64]int64, maxBucket)

	// Copy usefulRange
	s.usefulRange = usefulRange

	s.mid_num = make([]int64, 4, 4)
	s.bucket_sequence = make([]coreBucket, maxBucket)

	s.left_count = 0
	s.right_count = 0
	s.m = 0
	s.l = 0
	s.r = 0

	return s
}

func (cs *CoreSketch) InRange(v float64) bool {
	return (v > cs.usefulRange[0] && v < cs.usefulRange[1]) ||
		(v >= cs.usefulRange[2] && v < cs.usefulRange[3]) ||
		(v > cs.usefulRange[4] && v < cs.usefulRange[5])
}

func (coresketch *CoreSketch) InsertMid(v float64) {
	if v <= float64(coresketch.usefulRange[0]) {
		coresketch.mid_num[0] += 1
	} else if v >= float64(coresketch.usefulRange[1]) && v < float64(coresketch.usefulRange[2]) {
		coresketch.mid_num[1] += 1
	} else if v >= float64(coresketch.usefulRange[3]) && v < float64(coresketch.usefulRange[4]) {
		coresketch.mid_num[2] += 1
	} else if v >= float64(coresketch.usefulRange[5]) {
		coresketch.mid_num[3] += 1
	}
}

func (cs *CoreSketch) Customize(v float64) float64 {
	ur := cs.usefulRange
	if v < ur[0] {
		return ur[0]
	} else if v > ur[1] && v < ur[2] {
		return ur[2]
	} else if v > ur[3] && v < ur[4] {
		return ur[4]
	} else if v > ur[5] {
		return ur[5]
	}
	return v
}

func (coresketch *CoreSketch) Insert(v float64) {
	coresketch.data_size += 1
	if !coresketch.InRange(v) {
		coresketch.pruned_size += 1
		coresketch.InsertMid(v)
		v = coresketch.Customize(v)
	}
	i := int64(math.Ceil(coresketch.frac__power_2_card * math.Log(v)))
	coresketch.buckets[i] = coresketch.buckets[i] + 1
}

func (cs *CoreSketch) SerializeBucket() {
	i := 0
	zeta := math.Pow(2, math.Pow(2, -float64(cs.card)))
	cs.bucket_sequence = make([]coreBucket, len(cs.buckets))
	for k, v := range cs.buckets {
		lb := math.Pow(zeta, float64(k-1))
		ub := math.Pow(zeta, float64(k))
		cs.bucket_sequence[i] = coreBucket{index: k, count: v, lower_bound: lb, upper_bound: ub}
		i++
	}

	sort.Slice(cs.bucket_sequence, func(i, j int) bool {
		return cs.bucket_sequence[i].index < cs.bucket_sequence[j].index
	})
}

func (cs *CoreSketch) MidHalfCountBucket() int {
	cs.SerializeBucket()

	var count int64 = 0
	rank := 0.5 * float64(cs.data_size-1)

	for m := 0; m < len(cs.bucket_sequence); m++ {
		count += cs.bucket_sequence[m].count
		if float64(count) > rank {
			cs.m = m
			return m
		}
	}

	cs.m = -1
	return -1
}

// Find the median bucket
func (cs *CoreSketch) EdgeHalfCountBucket(m int) []int {
	count := cs.bucket_sequence[m].count
	cursor := m
	rank := 0.5 * float64(cs.data_size)
	l := m - 1
	r := m + 1
	cs.left_count += cs.bucket_sequence[m].count
	cs.right_count += cs.bucket_sequence[m].count

	// First while loop - compare l and r
	for count <= int64(rank) && l >= 0 && r < len(cs.bucket_sequence) {
		if cs.bucket_sequence[m].lower_bound-cs.bucket_sequence[l].upper_bound <
			cs.bucket_sequence[r].lower_bound-cs.bucket_sequence[m].upper_bound {
			cursor = l
			l-- // post-decrement
			cs.left_count += cs.bucket_sequence[cursor].count
		} else {
			cursor = r
			r++ // post-increment
			cs.right_count += cs.bucket_sequence[cursor].count
		}
		count += cs.bucket_sequence[cursor].count
	}

	// Second while loop - only decrement left
	for count <= int64(rank) && l >= 0 {
		cursor = l
		l-- // post-decrement
		cs.left_count += cs.bucket_sequence[cursor].count
		count += cs.bucket_sequence[cursor].count
	}

	// Third while loop - only increment right
	for count <= int64(rank) && r < len(cs.bucket_sequence) {
		cursor = r
		r++ // post-increment
		cs.right_count += cs.bucket_sequence[cursor].count
		count += cs.bucket_sequence[cursor].count
	}

	// Adjust l and r for edge conditions
	r--
	l++
	if r == len(cs.bucket_sequence)-1 && cursor != r {
		r = -1
	}
	if l == 0 && cursor != l {
		l = -1
	}

	cs.l = l
	cs.r = r

	// Return result as a slice of integers
	return []int{l, r}
}

func (cs *CoreSketch) RealRange() []float64 {
	realRange := make([]float64, 7)
	realRange[6] = 3
	realRange[0] = cs.usefulRange[0]
	realRange[1] = cs.usefulRange[1]

	cursor := 2

	if cs.usefulRange[2] < realRange[cursor-1] {
		realRange[cursor-1] = math.Max(cs.usefulRange[3], realRange[cursor-1])
		realRange[6] -= 1
	} else {
		realRange[cursor] = cs.usefulRange[2]
		realRange[cursor+1] = cs.usefulRange[3]
		cursor += 2
	}

	if cs.usefulRange[4] < realRange[cursor-1] {
		realRange[cursor-1] = math.Max(cs.usefulRange[5], realRange[cursor-1])
		realRange[6] -= 1
	} else {
		realRange[cursor] = cs.usefulRange[4]
		realRange[cursor+1] = cs.usefulRange[5]
	}

	return realRange
}

func (cs *CoreSketch) GenerateUsefulRange(m, l, r int) []float64 {
	// Create a slice with 6 elements, equivalent to the Java array.
	newRange := make([]float64, 6)

	// If l is -1
	if l == -1 {
		newRange[0] = math.Max(cs.usefulRange[0],
			2*cs.bucket_sequence[m].lower_bound-cs.bucket_sequence[r].upper_bound)
		newRange[1] = math.Min(cs.usefulRange[1],
			2*cs.bucket_sequence[m].upper_bound-cs.bucket_sequence[r].lower_bound)
		newRange[2] = math.Max(cs.usefulRange[2], cs.bucket_sequence[m].lower_bound)
		newRange[3] = math.Min(cs.usefulRange[3], cs.bucket_sequence[m].upper_bound)
		newRange[4] = math.Max(cs.usefulRange[4],
			cs.bucket_sequence[m].lower_bound+cs.bucket_sequence[r].lower_bound-cs.bucket_sequence[m].upper_bound)
		newRange[5] = math.Min(cs.usefulRange[5],
			cs.bucket_sequence[m].upper_bound+cs.bucket_sequence[r].upper_bound-cs.bucket_sequence[m].lower_bound)
	}

	// If r is -1
	if r == -1 {
		newRange[0] = math.Max(cs.usefulRange[0],
			cs.bucket_sequence[m].lower_bound-cs.bucket_sequence[m].upper_bound+cs.bucket_sequence[l].lower_bound)
		newRange[1] = math.Min(cs.usefulRange[1],
			cs.bucket_sequence[m].upper_bound-cs.bucket_sequence[m].lower_bound+cs.bucket_sequence[l].upper_bound)
		newRange[2] = math.Max(cs.usefulRange[2], cs.bucket_sequence[m].lower_bound)
		newRange[3] = math.Min(cs.usefulRange[3], cs.bucket_sequence[m].upper_bound)
		newRange[4] = math.Max(cs.usefulRange[4],
			2*cs.bucket_sequence[m].lower_bound-cs.bucket_sequence[l].upper_bound)
		newRange[5] = math.Min(cs.usefulRange[5],
			2*cs.bucket_sequence[m].upper_bound-cs.bucket_sequence[l].lower_bound)
	}

	// Final calculation when both l and r are valid
	newRange[0] = math.Max(cs.usefulRange[0], math.Min(
		cs.bucket_sequence[m].lower_bound-cs.bucket_sequence[m].upper_bound+cs.bucket_sequence[l].lower_bound,
		2*cs.bucket_sequence[m].lower_bound-cs.bucket_sequence[r].upper_bound))
	newRange[1] = math.Min(cs.usefulRange[1], math.Max(
		cs.bucket_sequence[m].upper_bound-cs.bucket_sequence[m].lower_bound+cs.bucket_sequence[l].upper_bound,
		2*cs.bucket_sequence[m].upper_bound-cs.bucket_sequence[r].lower_bound))
	newRange[2] = math.Max(cs.usefulRange[2], cs.bucket_sequence[m].lower_bound)
	newRange[3] = math.Min(cs.usefulRange[3], cs.bucket_sequence[m].upper_bound)
	newRange[4] = math.Max(cs.usefulRange[4], math.Min(
		2*cs.bucket_sequence[m].lower_bound-cs.bucket_sequence[l].upper_bound,
		cs.bucket_sequence[m].lower_bound+cs.bucket_sequence[r].lower_bound-cs.bucket_sequence[m].upper_bound))
	newRange[5] = math.Min(cs.usefulRange[5], math.Max(
		2*cs.bucket_sequence[m].upper_bound-cs.bucket_sequence[l].lower_bound,
		cs.bucket_sequence[m].upper_bound+cs.bucket_sequence[r].upper_bound-cs.bucket_sequence[m].lower_bound))

	return newRange
}

func (cs *CoreSketch) HalfCountBuckets() {
	m := cs.MidHalfCountBucket()
	cs.EdgeHalfCountBucket(m)
}

func (cs *CoreSketch) Merge(cs2 *CoreSketch) {
	diff := int64(math.Pow(2, math.Abs(float64(cs.card-cs2.card))))
	if cs.card <= cs2.card {
		for k, v := range cs2.buckets {
			cs.buckets[k] = v + cs.buckets[k/diff]
		}
	} else {
		for k, v := range cs.buckets {
			cs2.buckets[k] = v + cs.buckets[k/diff]
			cs.buckets = cs2.buckets
		}
		cs.card = cs2.card
		cs.power_2_card = cs2.power_2_card
		cs.frac__power_2_card = cs2.frac__power_2_card
	}
	// Combining ranges of sketches
	cs.usefulRange[0] = math.Min(cs.usefulRange[0], cs2.usefulRange[0])
	cs.usefulRange[1] = math.Min(cs.usefulRange[1], cs2.usefulRange[1])
	cs.usefulRange[2] = math.Min(cs.usefulRange[2], cs2.usefulRange[2])
	cs.usefulRange[3] = math.Min(cs.usefulRange[3], cs2.usefulRange[3])
	cs.usefulRange[4] = math.Min(cs.usefulRange[4], cs2.usefulRange[4])
	cs.usefulRange[5] = math.Min(cs.usefulRange[5], cs2.usefulRange[5])

	cs.data_size += cs2.data_size
	cs.pruned_size += cs2.pruned_size
}

func (cs *CoreSketch) UsefulCount() int64 {
	return cs.data_size - cs.pruned_size
}

func (cs *CoreSketch) GetBucketSize() int {
	return len(cs.buckets)
}

//TODO Use when needed?
//func (cs *CoreSketch) DataRead(parallel bool, threads int) bool {
//	if parallel {
//		return cs.UsefulCount()*8*threads <= int64(cs.maxBucket)*(32.125+16)
//	}
//	return cs.UsefulCount()*8 <= int64(cs.maxBucket)*(32.125+16)
//}

func (cs *CoreSketch) GetRange() []float64 {
	return cs.usefulRange
}

func (cs *CoreSketch) SetRange(rangeVal []float64) {
	cs.usefulRange = rangeVal
}

func (cs *CoreSketch) GetGap() []int64 {
	return cs.mid_num
}

func (cs *CoreSketch) SetCard(card int) {
	cs.card = card
	cs.power_2_card = math.Pow(2, float64(card))
	cs.frac__power_2_card = cs.power_2_card / LOG_2
}
