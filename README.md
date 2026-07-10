Bloom filters
-------------
[![Test](https://github.com/bits-and-blooms/bloom/actions/workflows/test.yml/badge.svg)](https://github.com/bits-and-blooms/bloom/actions/workflows/test.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/bits-and-blooms/bloom.svg)](https://pkg.go.dev/github.com/bits-and-blooms/bloom/v3)

## Presentation


A Bloom filter is a concise/compressed representation of a set, where the main
requirement is to make membership queries; _i.e._, whether an item is a
member of a set. A Bloom filter will always correctly report the presence
of an element in the set when the element is indeed present. A Bloom filter 
can use much less storage than the original set, but it allows for some 'false positives':
it may sometimes report that an element is in the set whereas it is not.

When you construct, you need to know how many elements you have (the desired capacity), and what is the desired false positive rate you are willing to tolerate. A common false-positive rate is 1%. The
lower the false-positive rate, the more memory you are going to require. Similarly, the higher the
capacity, the more memory you will use.
You may construct the Bloom filter capable of receiving 1 million elements with a false-positive
rate of 1% in the following manner. 

```Go
    filter := bloom.NewWithEstimates(1000000, 0.01) 
```

You should call `NewWithEstimates` conservatively: if you specify a number of elements that it is
too small, the false-positive bound might be exceeded. A Bloom filter is not a dynamic data structure:
you must know ahead of time what your desired capacity is.

Our implementation accepts keys for setting and testing as `[]byte`. Thus, to
add a string item, `"Love"`:

```Go
    filter.Add([]byte("Love"))
```

Similarly, to test if `"Love"` is in bloom:

```Go
    if filter.Test([]byte("Love"))
```

For numerical data, we recommend that you look into the encoding/binary library. But, for example, to add a `uint32` to the filter:

```Go
    i := uint32(100)
    n1 := make([]byte, 4)
    binary.BigEndian.PutUint32(n1, i)
    filter.Add(n1)
```

Godoc documentation:  https://pkg.go.dev/github.com/bits-and-blooms/bloom/v3 


## Installation

```bash
go get -u github.com/bits-and-blooms/bloom/v3
```

## Verifying the False Positive Rate


Sometimes, the actual false positive rate may differ (slightly) from the
theoretical false positive rate. We have a function to estimate the false positive rate of a
Bloom filter with _m_ bits and _k_ hashing functions for a set of size _n_:

```Go
    if bloom.EstimateFalsePositiveRate(20*n, 5, n) > 0.001 ...
```

You can use it to validate the computed m, k parameters:

```Go
    m, k := bloom.EstimateParameters(n, fp)
    ActualfpRate := bloom.EstimateFalsePositiveRate(m, k, n)
```

or

```Go
    f := bloom.NewWithEstimates(n, fp)
    ActualfpRate := bloom.EstimateFalsePositiveRate(f.m, f.k, n)
```

You would expect `ActualfpRate` to be close to the desired false-positive rate `fp` in these cases.

The `EstimateFalsePositiveRate` function creates a temporary Bloom filter. It is
also relatively expensive and only meant for validation.

## Serialization

You can read and write the Bloom filters as follows:


```Go
	f := New(1000, 4)
	var buf bytes.Buffer
	bytesWritten, err := f.WriteTo(&buf)
	if err != nil {
		t.Fatal(err.Error())
	}
	var g BloomFilter
	bytesRead, err := g.ReadFrom(&buf)
	if err != nil {
		t.Fatal(err.Error())
	}
	if bytesRead != bytesWritten {
		t.Errorf("read unexpected number of bytes %d != %d", bytesRead, bytesWritten)
	}
```

*Performance tip*: 
When reading and writing to a file or a network connection, you may get better performance by 
wrapping your streams with `bufio` instances.

E.g., 
```Go
	f, err := os.Create("myfile")
	w := bufio.NewWriter(f)
```
```Go
	f, err := os.Open("myfile")
	r := bufio.NewReader(f)
```

## Users


This library is used by popular systems such as


LSM-tree structures and index skipping:

- [github.com/milvus-io/milvus](https://github.com/milvus-io/milvus) — the flagship user explicitly named in the repo README; leading open-source vector database
- [github.com/weaviate/weaviate](https://github.com/weaviate/weaviate) — major vector DB, uses it in `lsmkv` (their LSM storage layer)
- [github.com/openGemini/openGemini](https://github.com/openGemini/openGemini) — open-source time-series DB, 3 index packages
- [github.com/siglens/siglens](https://github.com/siglens/siglens) — observability/log search engine, 5 packages
- [github.com/ankur-anand/unisondb](https://github.com/ankur-anand/unisondb) — embedded DB memtable
- [github.com/jakub-galecki/godb](https://github.com/jakub-galecki/godb) — LSM-tree DB SSTable layer

Observability & log processing:

- [github.com/grafana/loki](https://github.com/grafana/loki) — Grafana's log aggregation system, 4 packages in dataobj/index and storage layers

Distributed systems & consensus:

- [github.com/atomix/atomix](https://github.com/atomix/atomix) — distributed primitives framework (6 packages across 3 generations of the repo)
- [github.com/authzed/spicedb](https://github.com/authzed/spicedb) — SpiceDB permissions system (Zanzibar-style), dispatch layer
- [github.com/splitio/go-split-commons](https://github.com/splitio/go-split-commons) — Split.io Go SDK, 6 major versions all depend on it for impression deduplication

Networking & security tools:

- [github.com/projectdiscovery/hmap](https://github.com/projectdiscovery/hmap) — ProjectDiscovery's hmap (used across their whole nuclei/subfinder/httpx scanner ecosystem)
- [github.com/daeuniverse/dae](https://github.com/daeuniverse/dae) — Linux eBPF transparent proxy, routing table
- [github.com/bloXroute-Labs/gateway](https://github.com/bloXroute-Labs/gateway) — blockchain network gateway

Blockchain & crypto:

- [github.com/NethermindEth/juno](https://github.com/NethermindEth/juno) — Starknet full node (Go), 3 packages
- [github.com/iost-official/go-iost](https://github.com/iost-official/go-iost) — IOST blockchain p2p layer
- [github.com/status-im/status-go](https://github.com/status-im/status-go) — Status decentralized messenger, community protocol
- [github.com/code-payments/code-server](https://github.com/code-payments/code-server) — Code crypto payments backend

Privacy & identity:

- [github.com/optable/match](https://github.com/optable/match) — privacy-preserving record linkage (BPSI protocol)
- [github.com/Arceliar/ironwood](https://github.com/Arceliar/ironwood) — anonymous routing network library

Web crawling & scraping:

- [github.com/beego/beego](https://github.com/beego/beego) (via dependent ecosystem) — the web framework explicitly named in the README
- [github.com/editorpost/spider](https://github.com/editorpost/spider) — web spider deduplication store

Application infrastructure (URL shorteners, caches, feeds):

- [github.com/bentoml/yatai](https://github.com/bentoml/yatai) — BentoML model serving infrastructure
- [github.com/rotationalio/ensign](https://github.com/rotationalio/ensign) — event streaming platform
- [github.com/puppetlabs/leg](https://github.com/puppetlabs/leg) — Puppet Labs message deduplication middleware
- [github.com/letsencrypt/x509search](https://github.com/letsencrypt/x509search) — Let's Encrypt certificate search tool



## Contributing

If you wish to contribute to this project, please branch and issue a pull request against master ("[GitHub Flow](https://guides.github.com/introduction/flow/)")

This project includes a Makefile that allows you to test and build the project with simple commands.
To see all available options:
```bash
make help
```

## Running all tests

Before committing the code, please check if it passes all tests using (note: this will install some dependencies):
```bash
make deps
make qa
```

## Design

A Bloom filter has two parameters: _m_, the number of bits used in storage, and _k_, the number of hashing functions on elements of the set. (The actual hashing functions are important, too, but this is not a parameter for this implementation). A Bloom filter is backed by a [BitSet](https://github.com/bits-and-blooms/bitset); a key is represented in the filter by setting the bits at each value of the  hashing functions (modulo _m_). Set membership is done by _testing_ whether the bits at each value of the hashing functions (again, modulo _m_) are set. If so, the item is in the set. If the item is actually in the set, a Bloom filter will never fail (the true positive rate is 1.0); but it is susceptible to false positives. The art is to choose _k_ and _m_ correctly.

In this implementation, the hashing functions used is [murmurhash](github.com/twmb/murmur3), a non-cryptographic hashing function.


Given the particular hashing scheme, it's best to be empirical about this. Note
that estimating the FP rate will clear the Bloom filter.




### Goroutine safety

In general, it not safe to access
the same filter using different goroutines--they are
unsynchronized for performance. Should you want to access
a filter from more than one goroutine, you should
provide synchronization. Typically this is done by using channels (in Go style; so there is only ever one owner),
or by using `sync.Mutex` to serialize operations. Exceptionally, you may access the same filter from different
goroutines if you never modify the content of the filter.

## Further reading

<p>Mastering Programming: From Testing to Performance in Go</p>
<div><a href="https://www.amazon.com/dp/B0FMPGSWR5"><img style="margin-left: auto; margin-right: auto;" src="https://m.media-amazon.com/images/I/61feneHS7kL._SL1499_.jpg" alt="" width="250px" /></a></div>
