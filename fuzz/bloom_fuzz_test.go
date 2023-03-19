package fuzz_bloom

import (
	"bytes"
	"encoding/gob"
	fuzz "github.com/AdaLogics/go-fuzz-headers"
	"github.com/bits-and-blooms/bloom/v3"
	"math"
	"runtime/debug"
	"testing"
)

func RoundTripGobEncodeDecode(t *testing.T, filter *bloom.BloomFilter) {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(filter)
	if err != nil {
		t.Error(err)
	}
	dec := gob.NewDecoder(&buffer)
	var decoded_filter bloom.BloomFilter
	err = dec.Decode(&decoded_filter)
	if err != nil {
		t.Error(err)
	}
	if !decoded_filter.Equal(filter) {
		t.Errorf("Expected round trip encode/decode to result in identical bloom filters.")
	}
}

type FuzzData struct {
	ElementCount   uint64
	FalsePositives float64
	Add            [][]byte
	AddString      []string
	TestLocations  []uint64
	NotAdded       [][]byte
}

const (
	maxElements       = 10240
	minElements       = 2
	maxFalsePositives = 0.99
	minFalsePositives = 0.01
)

func FuzzBloom(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) {
		f := fuzz.NewConsumer(data)
		structuredData := FuzzData{}
		err := f.GenerateStruct(&structuredData)
		if err != nil {
			return
		}
		defer func() {
			if r := recover(); r != nil {
				t.Log(string(debug.Stack()))
				t.Fatalf("Failing input:\n %+v", structuredData)
			}
		}()

		// Limit input to max size.
		structuredData.ElementCount %= maxElements
		if structuredData.ElementCount == 0 {
			return
		}

		// Limit false positives.
		structuredData.FalsePositives = math.Mod(structuredData.FalsePositives, maxFalsePositives)
		if structuredData.FalsePositives < minFalsePositives || math.IsNaN(structuredData.FalsePositives) || math.IsInf(structuredData.FalsePositives, 0) {
			return
		}

		filter := bloom.NewWithEstimates(uint(structuredData.ElementCount), structuredData.FalsePositives)
		for _, to_add := range structuredData.Add {
			_ = filter.Add(to_add)
		}
		for _, to_add := range structuredData.AddString {
			_ = filter.AddString(to_add)
		}
		for _, to_test := range structuredData.Add {
			if !filter.Test(to_test) {
				t.Logf("Failed with structured input: %v", structuredData)
				t.Errorf("%v was added but was not reported as present in the set.", to_test)
			}
		}
		for _, to_test := range structuredData.AddString {
			if !filter.TestString(to_test) {
				t.Errorf("String '%s' was added but was not reported as present in the set.", to_test)
			}
		}

		for _, to_test := range structuredData.NotAdded {
			_ = filter.Test(to_test)
		}
		_ = filter.K()
		_ = filter.ApproximatedSize()
		_ = filter.Cap()
		_ = filter.TestLocations(structuredData.TestLocations)
		RoundTripGobEncodeDecode(t, filter)
	})
}