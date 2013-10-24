// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.package bloom

/*
A Bloom filter is a representation of a set of _n_ items, where the main
requirement is to make membership queries; _i.e._, whether an item is a
member of a set.

A Bloom filter has two parameters: _m_, a maximum size (typically a reasonably large
multiple of the cardinality of the set to represent) and _k_, the number of hashing
functions on elements of the set. (The actual hashing functions are important, too,
but this is not a parameter for this implementation). A Bloom filter is backed by
a BitSet; a key is represented in the filter by setting the bits at each value of the
hashing functions (modulo _m_). Set membership is done by _testing_ whether the
bits at each value of the hashing functions (again, modulo _m_) are set. If so,
the item is in the set. If the item is actually in the set, a Bloom filter will
never fail (the true positive rate is 1.0); but it is susceptible to false
positives. The art is to choose _k_ and _m_ correctly.

In this implementation, the hashing function used is FNV, a non-cryptographic
hashing function which is part of the Go package (hash/fnv). For a item, the
64-bit FNV hash is computed, and upper and lower 32 bit numbers, call them h1 and
h2, are used. Then, the _i_th hashing function is:

    h1 + h2*i

Thus, the underlying hash function, FNV, is only called once per key.

This implementation accepts keys for setting as testing as []byte. Thus, to
add a string item, "Love":

    uint n = 1000
    filter := bloom.New(20*n, 5) // load of 20, 5 keys
    filter.Add([]byte("Love"))

Similarly, to test if "Love" is in bloom:

    if filter.Test([]byte("Love"))

For numeric data, I recommend that you look into the binary/encoding library. But,
for example, to add a uint32 to the filter:

    i := uint32(100)
    n1 := make([]byte,4)
    binary.BigEndian.PutUint32(n1,i)
    f.Add(n1)

Finally, there is a method to estimate the false positive rate of a particular
bloom filter for a set of size _n_:

    if filter.EstimateFalsePositiveRate(1000) > 0.001

Given the particular hashing scheme, it's best to be empirical about this. Note
that estimating the FP rate will clear the Bloom filter.
*/
package bloom

import (
	"encoding/binary"
	"github.com/willf/bitset"
	"hash"
	"hash/fnv"
	"math"
	//"fmt"
)

type BloomFilter struct {
	m      uint
	k      uint
	b      *bitset.BitSet
	hasher hash.Hash64
}

// Create a new Bloom filter with _m_ bits and _k_ hashing functions
func New(m uint, k uint) *BloomFilter {
	return &BloomFilter{m, k, bitset.New(m), fnv.New64()}
}

// estimate parameters. Based on https://bitbucket.org/ww/bloom/src/829aa19d01d9/bloom.go
// used with permission.
func estimateParameters(n uint, p float64) (m uint, k uint) {
	m = uint(-1 * float64(n) * math.Log(p) / math.Pow(math.Log(2), 2))
	k = uint(math.Ceil(math.Log(2) * float64(m) / float64(n)))
	return
}

// Create a new Bloom filter for about n items with fp
// false positive rate
func NewWithEstimates(n uint, fp float64) *BloomFilter {
	m, k := estimateParameters(n, fp)
	return New(m, k)
}

// Return the capacity, _m_, of a Bloom filter
func (b *BloomFilter) Cap() uint {
	return b.m
}

// Return the number of hash functions used
func (b *BloomFilter) K() uint {
	return b.k
}

// get the two basic hash function values for data
func (f *BloomFilter) base_hashes(data []byte) (a uint32, b uint32) {
	f.hasher.Reset()
	f.hasher.Write(data)
	sum := f.hasher.Sum(nil)
	upper := sum[0:4]
	lower := sum[4:8]
	a = binary.BigEndian.Uint32(lower)
	b = binary.BigEndian.Uint32(upper)
	return
}

// get the _k_ locations to set/test in the underlying bitset
func (f *BloomFilter) locations(data []byte) (locs []uint) {
	locs = make([]uint, f.k)
	a, b := f.base_hashes(data)
	ua := uint(a)
	ub := uint(b)
	//fmt.Println(ua, ub)
	for i := uint(0); i < f.k; i++ {
		locs[i] = (ua + ub*i) % f.m
	}
	//fmt.Println(data, "->", locs)
	return
}

// Add data to the Bloom Filter. Returns the filter (allows chaining)
func (f *BloomFilter) Add(data []byte) *BloomFilter {
	for _, loc := range f.locations(data) {
		f.b.Set(loc)
	}
	return f
}

// Tests for the presence of data in the Bloom filter
func (f *BloomFilter) Test(data []byte) bool {
	for _, loc := range f.locations(data) {
		if !f.b.Test(loc) {
			return false
		}
	}
	return true
}

// Equivalent to calling Test(data) then Add(data).  Returns the result of Test.
func (f *BloomFilter) TestAndAdd(data []byte) bool {
	present := true
	for _, loc := range f.locations(data) {
		if !f.b.Test(loc) {
			present = false
		}
		f.b.Set(loc)
	}
	return present
}

// Clear all the data in a Bloom filter, removing all keys
func (f *BloomFilter) ClearAll() *BloomFilter {
	f.b.ClearAll()
	return f
}

// Estimate, for a BloomFilter with a limit of m bytes
// and k hash functions, what the false positive rate will be
// whilst storing n entries; runs 10k tests
func (f *BloomFilter) EstimateFalsePositiveRate(n uint) (fp_rate float64) {
	f.ClearAll()
	n1 := make([]byte, 4)
	for i := uint32(0); i < uint32(n); i++ {
		binary.BigEndian.PutUint32(n1, i)
		f.Add(n1)
	}
	fp := 0
	// test 10k numbers
	for i := uint32(0); i < uint32(10000); i++ {
		binary.BigEndian.PutUint32(n1, i+uint32(n)+1)
		if f.Test(n1) {
			fp++
		}
	}
	fp_rate = float64(fp) / float64(100)
	f.ClearAll()
	return
}
