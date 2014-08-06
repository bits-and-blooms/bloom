Bloom filters
-------------

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
                                                         
Discussion here: [Bloom filter](https://groups.google.com/d/topic/golang-nuts/6MktecKi1bE/discussion)

Godoc documentation at https://godoc.org/github.com/willf/bloom
