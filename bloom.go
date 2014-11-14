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
	"bytes"
	"encoding/binary"
	"encoding/json"
	"hash"
	"hash/fnv"
	"io"
	"math"

	"github.com/willf/bitset"
	//"fmt"
)

type BloomFilter struct {
	m       uint
	k       uint
	b       *bitset.BitSet
	locBuff []uint
	present bool
	hasher  hash.Hash64
}

// Create a new Bloom filter with _m_ bits and _k_ hashing functions
func New(m uint, k uint) *BloomFilter {

	return &BloomFilter{m, k, bitset.New(m), make([]uint, k), false, fnv.New64()}
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
func (f *BloomFilter) Cap() uint {
	return f.m
}

// Return the number of hash functions used
func (f *BloomFilter) K() uint {
	return f.k
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
func (f *BloomFilter) locations(data []byte) {
	a, b := f.base_hashes(data)
	ua := uint(a)
	ub := uint(b)
	//fmt.Println(ua, ub)
	for i := uint(0); i < f.k; i++ {
		f.locBuff[i] = (ua + ub*i) % f.m
	}
	//fmt.Println(data, "->", locs)
	return
}

// Add data to the Bloom Filter. Returns the filter (allows chaining)
func (f *BloomFilter) Add(data []byte) *BloomFilter {
	f.locations(data)
	for i := uint(0); i < f.k; i++ {
		f.b.Set(f.locBuff[i])
	}

	return f
}

// Tests for the presence of data in the Bloom filter
func (f *BloomFilter) Test(data []byte) bool {
	f.locations(data)
	for i := uint(0); i < f.k; i++ {
		if !f.b.Test(f.locBuff[i]) {
			return false
		}
	}
	return true
}

// Equivalent to calling Test(data) then Add(data).  Returns the result of Test.
func (f *BloomFilter) TestAndAdd(data []byte) bool {
	f.locations(data)
	f.present = true
	for i := uint(0); i < f.k; i++ {
		if !f.b.Test(f.locBuff[i]) {
			f.present = false
		}
		f.b.Set(f.locBuff[i])
	}
	return f.present
}

// Clear all the data in a Bloom filter, removing all keys
func (f *BloomFilter) ClearAll() *BloomFilter {
	f.b.ClearAll()
	return f
}

// Estimate, for a BloomFilter with a limit of m bytes
// and k hash functions, what the false positive rate will be
// whilst storing n entries; runs n * 2 tests.
func (f *BloomFilter) EstimateFalsePositiveRate(n uint) (fp_rate float64) {
	rounds := uint32(n * 2)
	f.ClearAll()
	n1 := make([]byte, 4)
	for i := uint32(0); i < uint32(n); i++ {
		binary.BigEndian.PutUint32(n1, i)
		f.Add(n1)
	}
	fp := 0
	// test for number of rounds
	for i := uint32(0); i < rounds; i++ {
		binary.BigEndian.PutUint32(n1, i+uint32(n)+1)
		if f.Test(n1) {
			fp++
		}
	}
	fp_rate = float64(fp) / float64(100)
	f.ClearAll()
	return
}

// bloomFilterJSON is an unexported type for marshaling/unmarshaling BloomFilter struct.
type bloomFilterJSON struct {
	M uint           `json:"m"`
	K uint           `json:"k"`
	B *bitset.BitSet `json:"b"`
}

// MarshalJSON implements json.Marshaler interface.
func (f *BloomFilter) MarshalJSON() ([]byte, error) {
	return json.Marshal(bloomFilterJSON{f.m, f.k, f.b})
}

// UnmarshalJSON implements json.Unmarshaler interface.
func (f *BloomFilter) UnmarshalJSON(data []byte) error {
	var j bloomFilterJSON
	err := json.Unmarshal(data, &j)
	if err != nil {
		return err
	}
	f.m = j.M
	f.k = j.K
	f.b = j.B
	f.hasher = fnv.New64()
	f.locBuff = make([]uint, f.k)
	return nil
}

// WriteTo writes a binary representation of the BloomFilter to an i/o stream.
// It returns the number of bytes written.
func (f *BloomFilter) WriteTo(stream io.Writer) (int64, error) {
	err := binary.Write(stream, binary.BigEndian, uint64(f.m))
	if err != nil {
		return 0, err
	}
	err = binary.Write(stream, binary.BigEndian, uint64(f.k))
	if err != nil {
		return 0, err
	}
	numBytes, err := f.b.WriteTo(stream)
	return numBytes + int64(2*binary.Size(uint64(0))), err
}

// ReadFrom reads a binary representation of the BloomFilter (such as might
// have been written by WriteTo()) from an i/o stream. It returns the number
// of bytes read.
func (f *BloomFilter) ReadFrom(stream io.Reader) (int64, error) {
	var m, k uint64
	err := binary.Read(stream, binary.BigEndian, &m)
	if err != nil {
		return 0, err
	}
	err = binary.Read(stream, binary.BigEndian, &k)
	if err != nil {
		return 0, err
	}
	b := &bitset.BitSet{}
	numBytes, err := b.ReadFrom(stream)
	if err != nil {
		return 0, err
	}
	f.m = uint(m)
	f.k = uint(k)
	f.b = b
	f.hasher = fnv.New64()
	return numBytes + int64(2*binary.Size(uint64(0))), nil
}

// GobEncode implements gob.GobEncoder interface.
func (f *BloomFilter) GobEncode() ([]byte, error) {
	var buf bytes.Buffer
	_, err := f.WriteTo(&buf)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// GobDecode implements gob.GobDecoder interface.
func (f *BloomFilter) GobDecode(data []byte) error {
	buf := bytes.NewBuffer(data)
	_, err := f.ReadFrom(buf)
	f.locBuff = make([]uint, f.k)
	return err
}
