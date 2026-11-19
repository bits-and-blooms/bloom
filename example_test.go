package bloom_test

import (
	"fmt"

	"github.com/bits-and-blooms/bloom/v3"
)

func Example() {
	// A filter for up to 1000 items with a 1% false-positive rate.
	filter := bloom.NewWithEstimates(1000, 0.01)
	filter.Add([]byte("Love"))
	fmt.Println(filter.Test([]byte("Love")))
	fmt.Println(filter.TestString("Hate"))
	// Output:
	// true
	// false
}

func ExampleEstimateParameters() {
	m, k := bloom.EstimateParameters(1000, 0.01)
	fmt.Println(m, k)
	// Output: 9586 7
}
