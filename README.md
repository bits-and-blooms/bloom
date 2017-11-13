Bloom filters
-------------

## Note: This is a fork of [github.com/willf/bloom](http://github.com/willf/bloom) that provides a read only bloom filter and a concurrent read only bloom filter that can both be instantiated from a mmap'd bytes ref and also limits the scope of the API.

[![Master Build Status](https://secure.travis-ci.org/m3db/bloom.png?branch=master)](https://travis-ci.org/m3db/bloom?branch=master)
[![Coverage Status](https://coveralls.io/repos/github/m3db/bloom/badge.svg?branch=master)](https://coveralls.io/github/m3db/bloom?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/m3db/bloom)](https://goreportcard.com/report/github.com/m3db/bloom)
[![GoDoc](https://godoc.org/github.com/m3db/bloom?status.svg)](http://godoc.org/github.com/m3db/bloom)

A Bloom filter is a representation of a set of _n_ items, where the main
requirement is to make membership queries; _i.e._, whether an item is a
member of a set.

A Bloom filter has two parameters: _m_, a maximum size (typically a reasonably large multiple of the cardinality of the set to represent) and _k_, the number of hashing functions on elements of the set. (The actual hashing functions are important, too, but this is not a parameter for this implementation). A Bloom filter is backed by a [BitSet](http://github.com/m3db/bitset); a key is represented in the filter by setting the bits at each value of the  hashing functions (modulo _m_). Set membership is done by _testing_ whether the bits at each value of the hashing functions (again, modulo _m_) are set. If so, the item is in the set. If the item is actually in the set, a Bloom filter will never fail (the true positive rate is 1.0); but it is susceptible to false positives. The art is to choose _k_ and _m_ correctly.

In this implementation, the hashing functions used is [murmurhash](http://github.com/spaolacci/murmur3), a non-cryptographic hashing function.

This implementation accepts keys for setting and testing as `[]byte`. Thus, to
add a string item, `"Love"`:

    n := uint(1000)
    filter := bloom.NewBloomFilter(20*n, 5) // load of 20, 5 keys
    filter.Add([]byte("Love"))

Similarly, to test if `"Love"` is in bloom:

    if filter.Test([]byte("Love"))

Godoc documentation: https://godoc.org/github.com/m3db/bloom

## Installation

```bash
go get -u github.com/m3db/bloom
```

## Running all tests

Before committing the code, please check if it passes all tests using (note: this will install some dependencies):
```bash
make deps
make qa
```
