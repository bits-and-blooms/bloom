/*
Package bloom provides data structures and methods for creating Bloom filters.

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

In this implementation, the hashing functions used is murmurhash,
a non-cryptographic hashing function.

This implementation accepts keys for setting as testing as []byte. Thus, to
add a string item, "Love":

    uint n = 1000
    filter := bloom.New(20*n, 5) // load of 20, 5 keys
    filter.Add([]byte("Love"))

Similarly, to test if "Love" is in bloom:

    if filter.Test([]byte("Love"))
*/
package bloom

import (
	"math"

	"github.com/m3db/bitset"
	"github.com/twmb/murmur3"
)

var entropy = []byte{1}[:]

func concurrentBloomFilterHashes(data []byte) [4]uint64 {
	h1, h2 := murmur3.Sum128(data)
	h3, h4 := murmur3.Sum128(entropy) // Add entropy
	return [4]uint64{h1, h2, h3, h4}
}

func bloomFilterLocation(h [4]uint64, i, m uint64) uint {
	v := h[i%2] + i*h[2+(((i+(i%2))%4)/2)]
	return uint(v % m)
}

// BloomFilter is a bloom filter set membership.
// It cannot be concurrently read or written to. Multiple concurrent readers
// is also unsafe so a sync.Mutex must be used to guard read/write access if
// desired. This is due to a cached hash digest being used to avoid
// allocation each read/write.
type BloomFilter struct {
	m   uint64
	k   uint64
	set *bitset.BitSet
}

// NewBloomFilter creates a new bloom filter that can represent
// m elements with k hashes. It is not concurrent read or write safe.
func NewBloomFilter(m uint, k uint) *BloomFilter {
	if m < 1 {
		m = 1
	}
	if k < 1 {
		k = 1
	}
	return &BloomFilter{
		m:   uint64(m),
		k:   uint64(k),
		set: bitset.NewBitSet(m),
	}
}

// EstimateFalsePositiveRate estimates m and k, based on:
// https://stackoverflow.com/a/22467497
func EstimateFalsePositiveRate(n uint, p float64) (m uint, k uint) {
	floatM := (float64(-1) * float64(n) * math.Log(p)) / (math.Pow(math.Log(2), 2))
	floatK := (floatM / float64(n)) * math.Log(2)
	m, k = uint(math.Ceil(floatM)), uint(math.Ceil(floatK))
	return
}

// Add value to the set.
func (b *BloomFilter) Add(value []byte) {
	h := concurrentBloomFilterHashes(value)
	for i := uint64(0); i < b.k; i++ {
		b.set.Set(bloomFilterLocation(h, i, b.m))
	}
}

// Test if value is in the set.
func (b *BloomFilter) Test(value []byte) bool {
	h := concurrentBloomFilterHashes(value)
	for i := uint64(0); i < b.k; i++ {
		if !b.set.Test(bloomFilterLocation(h, i, b.m)) {
			return false
		}
	}
	return true
}

// M returns the m elements represented.
func (b *BloomFilter) M() uint {
	return uint(b.m)
}

// K returns the k hashes used.
func (b *BloomFilter) K() uint {
	return uint(b.k)
}

// BitSet returns the bitset used.
func (b *BloomFilter) BitSet() *bitset.BitSet {
	return b.set
}

// ReadOnlyBloomFilter is a read only bloom filter set membership.
// It cannot be concurrently read or written to. Multiple concurrent readers
// is also unsafe so a sync.Mutex must be used to guard read/write access if
// desired. This is due to a cached hash digest being used to avoid
// allocation each read/write.
type ReadOnlyBloomFilter struct {
	m   uint64
	k   uint64
	set *bitset.ReadOnlyBitSet
}

// NewReadOnlyBloomFilter returns a new read only bloom filter backed
// by a byte slice, this means it can be used with a mmap'd bytes ref.
// It is not concurrent read or write safe.
func NewReadOnlyBloomFilter(m, k uint, data []byte) *ReadOnlyBloomFilter {
	return &ReadOnlyBloomFilter{
		m:   uint64(m),
		k:   uint64(k),
		set: bitset.NewReadOnlyBitSet(data),
	}
}

// Test if value is in the set.
func (b *ReadOnlyBloomFilter) Test(value []byte) bool {
	h := concurrentBloomFilterHashes(value)
	for i := uint64(0); i < b.k; i++ {
		if !b.set.Test(bloomFilterLocation(h, i, b.m)) {
			return false
		}
	}
	return true
}

// M returns the m elements represented.
func (b *ReadOnlyBloomFilter) M() uint {
	return uint(b.m)
}

// K returns the k hashes used.
func (b *ReadOnlyBloomFilter) K() uint {
	return uint(b.k)
}

// BitSet returns the bitset used.
func (b *ReadOnlyBloomFilter) BitSet() *bitset.ReadOnlyBitSet {
	return b.set
}

// ConcurrentReadOnlyBloomFilter is a concurrent read only bloom filter set
// membership. It can be concurrently read from by any number of readers.
type ConcurrentReadOnlyBloomFilter struct {
	m   uint64
	k   uint64
	set *bitset.ReadOnlyBitSet
}

// NewConcurrentReadOnlyBloomFilter returns a new concurrent read only bloom
// filter backed by a byte slice, this means it can be used with a mmap'd
// bytes ref. It can be concurrently read from by any number of readers.
func NewConcurrentReadOnlyBloomFilter(
	m, k uint,
	data []byte,
) *ConcurrentReadOnlyBloomFilter {
	return &ConcurrentReadOnlyBloomFilter{
		m:   uint64(m),
		k:   uint64(k),
		set: bitset.NewReadOnlyBitSet(data),
	}
}

// Test if value is in the set.
func (b *ConcurrentReadOnlyBloomFilter) Test(value []byte) bool {
	h := concurrentBloomFilterHashes(value)
	for i := uint64(0); i < b.k; i++ {
		if !b.set.Test(bloomFilterLocation(h, i, b.m)) {
			return false
		}
	}
	return true
}

// M returns the m elements represented.
func (b *ConcurrentReadOnlyBloomFilter) M() uint {
	return uint(b.m)
}

// K returns the k hashes used.
func (b *ConcurrentReadOnlyBloomFilter) K() uint {
	return uint(b.k)
}

// BitSet returns the bitset used.
func (b *ConcurrentReadOnlyBloomFilter) BitSet() *bitset.ReadOnlyBitSet {
	return b.set
}
