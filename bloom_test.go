package bloom

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"runtime"
	"sync"
	"testing"

	"github.com/spaolacci/murmur3"

	"github.com/stretchr/testify/require"
)

// TestConcurrent must be run with -race to detect failures
func TestConcurrent(t *testing.T) {
	gmp := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(gmp)

	f := NewBloomFilter(1000, 4)
	n1 := []byte("Bess")
	n2 := []byte("Jane")
	f.Add(n1)
	f.Add(n2)

	buf := bytes.NewBuffer(nil)
	require.NoError(t, f.BitSet().Write(buf))

	ro := NewConcurrentReadOnlyBloomFilter(f.M(), f.K(), buf.Bytes())
	require.Equal(t, f.M(), ro.M())
	require.Equal(t, f.K(), ro.K())

	var wg sync.WaitGroup
	const try = 1000
	var err1, err2 error

	wg.Add(1)
	go func() {
		for i := 0; i < try; i++ {
			n1b := ro.Test(n1)
			if !n1b {
				err1 = fmt.Errorf("%v should be in", n1)
				break
			}
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		for i := 0; i < try; i++ {
			n2b := ro.Test(n2)
			if !n2b {
				err2 = fmt.Errorf("%v should be in", n2)
				break
			}
		}
		wg.Done()
	}()

	wg.Wait()

	if err1 != nil {
		t.Fatal(err1)
	}
	if err2 != nil {
		t.Fatal(err2)
	}
}

func TestBasic(t *testing.T) {
	f := NewBloomFilter(1000, 4)
	n1 := []byte("Bess")
	n2 := []byte("Jane")
	n3 := []byte("Emma")
	f.Add(n1)
	n3a := f.Test(n3)
	f.Add(n3)
	n1b := f.Test(n1)
	n2b := f.Test(n2)
	n3b := f.Test(n3)
	if !n1b {
		t.Errorf("%v should be in.", n1)
	}
	if n2b {
		t.Errorf("%v should not be in.", n2)
	}
	if n3a {
		t.Errorf("%v should not be in the first time we look.", n3)
	}
	if !n3b {
		t.Errorf("%v should be in the second time we look.", n3)
	}
}

func TestReadOnly(t *testing.T) {
	f := NewBloomFilter(1000, 4)
	n1 := []byte("Bess")
	n2 := []byte("Jane")
	n3 := []byte("Emma")
	f.Add(n1)
	f.Add(n3)

	buf := bytes.NewBuffer(nil)
	require.NoError(t, f.BitSet().Write(buf))

	ro := NewReadOnlyBloomFilter(f.M(), f.K(), buf.Bytes())
	require.Equal(t, f.M(), ro.M())
	require.Equal(t, f.K(), ro.K())

	n1b := ro.Test(n1)
	n2b := ro.Test(n2)
	n3b := ro.Test(n3)
	if !n1b {
		t.Errorf("%v should be in.", n1)
	}
	if n2b {
		t.Errorf("%v should not be in.", n2)
	}
	if !n3b {
		t.Errorf("%v should be in the second time we look.", n3)
	}
}

func TestBasicUint32(t *testing.T) {
	f := NewBloomFilter(1000, 4)
	n1 := make([]byte, 4)
	n2 := make([]byte, 4)
	n3 := make([]byte, 4)
	n4 := make([]byte, 4)
	binary.BigEndian.PutUint32(n1, 100)
	binary.BigEndian.PutUint32(n2, 101)
	binary.BigEndian.PutUint32(n3, 102)
	binary.BigEndian.PutUint32(n4, 103)
	f.Add(n1)
	n3a := f.Test(n3)
	f.Add(n3)
	n1b := f.Test(n1)
	n2b := f.Test(n2)
	n3b := f.Test(n3)
	f.Test(n4)
	if !n1b {
		t.Errorf("%v should be in.", n1)
	}
	if n2b {
		t.Errorf("%v should not be in.", n2)
	}
	if n3a {
		t.Errorf("%v should not be in the first time we look.", n3)
	}
	if !n3b {
		t.Errorf("%v should be in the second time we look.", n3)
	}
}

func TestNewWithLowNumbers(t *testing.T) {
	f := NewBloomFilter(0, 0)
	if f.k != 1 {
		t.Errorf("%v should be 1", f.k)
	}
	if f.m != 1 {
		t.Errorf("%v should be 1", f.m)
	}
}

func TestString(t *testing.T) {
	m, k := EstimateFalsePositiveRate(1000, 0.001)
	f := NewBloomFilter(m, k)
	n1 := "Love"
	n2 := "is"
	n3 := "in"
	n4 := "bloom"
	f.Add([]byte(n1))
	n3a := f.Test([]byte(n3))
	f.Add([]byte(n3))
	n1b := f.Test([]byte(n1))
	n2b := f.Test([]byte(n2))
	n3b := f.Test([]byte(n3))
	f.Test([]byte(n4))
	if !n1b {
		t.Errorf("%v should be in.", n1)
	}
	if n2b {
		t.Errorf("%v should not be in.", n2)
	}
	if n3a {
		t.Errorf("%v should not be in the first time we look.", n3)
	}
	if !n3b {
		t.Errorf("%v should be in the second time we look.", n3)
	}
}

func min(a, b uint) uint {
	if a < b {
		return a
	}
	return b
}

// The following function courtesy of Nick @turgon
// This helper function ranges over the input data, applying the hashing
// which returns the bit locations to set in the filter.
// For each location, increment a counter for that bit address.
//
// If the Bloom Filter's location() method distributes locations uniformly
// at random, a property it should inherit from its hash function, then
// each bit location in the filter should end up with roughly the same
// number of hits.  Importantly, the value of k should not matter.
//
// Once the results are collected, we can run a chi squared goodness of fit
// test, comparing the result histogram with the uniform distribition.
// This yields a test statistic with degrees-of-freedom of m-1.
func chiTestBloom(m, k, rounds uint, elements [][]byte) (succeeds bool) {
	f := NewBloomFilter(m, k)
	results := make([]uint, m)
	chi := make([]float64, m)

	hash := murmur3.New128()
	for _, data := range elements {
		hash.Reset()
		h := bloomFilterHashes(hash, data)
		for i := uint64(0); i < f.k; i++ {
			results[bloomFilterLocation(h, i, f.m)]++
		}
	}

	// Each element of results should contain the same value: k * rounds / m.
	// Let's run a chi-square goodness of fit and see how it fares.
	var chiStatistic float64
	e := float64(k*rounds) / float64(m)
	for i := uint(0); i < m; i++ {
		chi[i] = math.Pow(float64(results[i])-e, 2.0) / e
		chiStatistic += chi[i]
	}

	// this tests at significant level 0.005 up to 20 degrees of freedom
	table := [20]float64{
		7.879, 10.597, 12.838, 14.86, 16.75, 18.548, 20.278,
		21.955, 23.589, 25.188, 26.757, 28.3, 29.819, 31.319, 32.801, 34.267,
		35.718, 37.156, 38.582, 39.997}
	df := min(m-1, 20)

	succeeds = table[df-1] > chiStatistic
	return

}

func TestLocation(t *testing.T) {
	var m, k, rounds uint

	m = 8
	k = 3

	rounds = 100000 // 15000000

	elements := make([][]byte, rounds)

	for x := uint(0); x < rounds; x++ {
		ctrlist := make([]uint8, 4)
		ctrlist[0] = uint8(x)
		ctrlist[1] = uint8(x >> 8)
		ctrlist[2] = uint8(x >> 16)
		ctrlist[3] = uint8(x >> 24)
		data := []byte(ctrlist)
		elements[x] = data
	}

	succeeds := chiTestBloom(m, k, rounds, elements)
	if !succeeds {
		t.Error("random assignment is too unrandom")
	}

}
