package bloom_test

import (
	"fmt"
	"github.com/bits-and-blooms/bloom/v3"
)

func Example() {
	filter := bloom.NewWithEstimates(1000000, 0.01)
	filter.Add([]byte("Love"))
	fmt.Println(filter.Test([]byte("Love")))

	var n uint = 1_000_000
	fmt.Println(bloom.EstimateFalsePositiveRate(20*n, 5, n) > 0.001)

	expectedFpRate := 0.01
	m, k := bloom.EstimateParameters(n, expectedFpRate)
	actualFpRate := bloom.EstimateFalsePositiveRate(m, k, n)
	fmt.Printf("expectedFpRate=%v, actualFpRate=%v\n", expectedFpRate, actualFpRate)
}
