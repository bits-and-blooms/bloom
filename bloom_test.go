package bloom

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"testing"
)

func TestConcurrent(t *testing.T) {
	gmp := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(gmp)

	f := New(1000, 4)
	n1 := []byte("Bess")
	n2 := []byte("Jane")
	f.Add(n1)
	f.Add(n2)

	var wg sync.WaitGroup
	const try = 1000
	var err1, err2 error

	wg.Add(1)
	go func() {
		for i := 0; i < try; i++ {
			n1b := f.Test(n1)
			if !n1b {
				err1 = fmt.Errorf("%v should be in.", n1)
				break
			}
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		for i := 0; i < try; i++ {
			n2b := f.Test(n2)
			if !n2b {
				err2 = fmt.Errorf("%v should be in.", n2)
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
	f := New(1000, 4)
	n1 := []byte("Bess")
	n2 := []byte("Jane")
	n3 := []byte("Emma")
	f.Add(n1)
	n3a := f.TestAndAdd(n3)
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

func TestBasicUint32(t *testing.T) {
	f := New(1000, 4)
	n1 := make([]byte, 4)
	n2 := make([]byte, 4)
	n3 := make([]byte, 4)
	n4 := make([]byte, 4)
	binary.BigEndian.PutUint32(n1, 100)
	binary.BigEndian.PutUint32(n2, 101)
	binary.BigEndian.PutUint32(n3, 102)
	binary.BigEndian.PutUint32(n4, 103)
	f.Add(n1)
	n3a := f.TestAndAdd(n3)
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

func TestDirect20_5(t *testing.T) {
	n := uint(10000)
	k := uint(5)
	load := uint(20)
	f := New(n*load, k)
	fp_rate := f.EstimateFalsePositiveRate(n)
	if fp_rate > 0.0001 {
		t.Errorf("False positive rate too high: load=%v, k=%v, %f", load, k, fp_rate)
	}
}

func TestDirect15_10(t *testing.T) {
	n := uint(10000)
	k := uint(10)
	load := uint(15)
	f := New(n*load, k)
	fp_rate := f.EstimateFalsePositiveRate(n)
	if fp_rate > 0.0001 {
		t.Errorf("False positive rate too high: load=%v, k=%v, %f", load, k, fp_rate)
	}
}

func TestEstimated10_0001(t *testing.T) {
	n := uint(10000)
	fp := 0.0001
	m, k := estimateParameters(n, fp)
	f := NewWithEstimates(n, fp)
	fp_rate := f.EstimateFalsePositiveRate(n)
	if fp_rate > fp {
		t.Errorf("False positive rate too high: n: %v, fp: %f, n: %v, k: %v result: %f", n, fp, m, k, fp_rate)
	}
}

func TestEstimated10_001(t *testing.T) {
	n := uint(10000)
	fp := 0.001
	m, k := estimateParameters(n, fp)
	f := NewWithEstimates(n, fp)
	fp_rate := f.EstimateFalsePositiveRate(n)
	if fp_rate > fp {
		t.Errorf("False positive rate too high: n: %v, fp: %f, n: %v, k: %v result: %f", n, fp, m, k, fp_rate)
	}
}

func TestMarshalUnmarshalJSON(t *testing.T) {
	f := New(1000, 4)
	data, err := json.Marshal(f)
	if err != nil {
		t.Fatal(err.Error())
	}

	var g BloomFilter
	err = json.Unmarshal(data, &g)
	if err != nil {
		t.Fatal(err.Error())
	}
	if g.m != f.m {
		t.Error("invalid m value")
	}
	if g.k != f.k {
		t.Error("invalid k value")
	}
	if g.b == nil {
		t.Fatal("bitset is nil")
	}
	if !g.b.Equal(f.b) {
		t.Error("bitsets are not equal")
	}
}

func TestWriteToReadFrom(t *testing.T) {
	var b bytes.Buffer
	f := New(1000, 4)
	_, err := f.WriteTo(&b)
	if err != nil {
		t.Fatal(err)
	}

	g := New(1000, 1)
	_, err = g.ReadFrom(&b)
	if err != nil {
		t.Fatal(err)
	}
	if g.m != f.m {
		t.Error("invalid m value")
	}
	if g.k != f.k {
		t.Error("invalid k value")
	}
	if g.b == nil {
		t.Fatal("bitset is nil")
	}
	if !g.b.Equal(f.b) {
		t.Error("bitsets are not equal")
	}

	g.Test([]byte(""))
}

func TestReadWriteBinary(t *testing.T) {
	f := New(1000, 4)
	var buf bytes.Buffer
	bytesWritten, err := f.WriteTo(&buf)
	if err != nil {
		t.Fatal(err.Error())
	}
	if bytesWritten != int64(buf.Len()) {
		t.Errorf("incorrect write length %d != %d", bytesWritten, buf.Len())
	}

	var g BloomFilter
	bytesRead, err := g.ReadFrom(&buf)
	if err != nil {
		t.Fatal(err.Error())
	}
	if bytesRead != bytesWritten {
		t.Errorf("read unexpected number of bytes %d != %d", bytesRead, bytesWritten)
	}
	if g.m != f.m {
		t.Error("invalid m value")
	}
	if g.k != f.k {
		t.Error("invalid k value")
	}
	if g.b == nil {
		t.Fatal("bitset is nil")
	}
	if !g.b.Equal(f.b) {
		t.Error("bitsets are not equal")
	}
}

func TestEncodeDecodeGob(t *testing.T) {
	f := New(1000, 4)
	f.Add([]byte("one"))
	f.Add([]byte("two"))
	f.Add([]byte("three"))
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(f)
	if err != nil {
		t.Fatal(err.Error())
	}

	var g BloomFilter
	err = gob.NewDecoder(&buf).Decode(&g)
	if err != nil {
		t.Fatal(err.Error())
	}
	if g.m != f.m {
		t.Error("invalid m value")
	}
	if g.k != f.k {
		t.Error("invalid k value")
	}
	if g.b == nil {
		t.Fatal("bitset is nil")
	}
	if !g.b.Equal(f.b) {
		t.Error("bitsets are not equal")
	}
	if !g.Test([]byte("three")) {
		t.Errorf("missing value 'three'")
	}
	if !g.Test([]byte("two")) {
		t.Errorf("missing value 'two'")
	}
	if !g.Test([]byte("one")) {
		t.Errorf("missing value 'one'")
	}
}

func BenchmarkDirect(b *testing.B) {
	n := uint(10000)
	max_k := uint(10)
	max_load := uint(20)
	fmt.Printf("m/n")
	for k := uint(2); k <= max_k; k++ {
		fmt.Printf("\tk=%v", k)
	}
	fmt.Println()
	for load := uint(2); load <= max_load; load++ {
		fmt.Print(load)
		for k := uint(2); k <= max_k; k++ {
			f := New(n*load, k)
			fp_rate := f.EstimateFalsePositiveRate(n)
			fmt.Printf("\t%f", fp_rate)
		}
		fmt.Println()
	}
}

func BenchmarkEstimated(b *testing.B) {
	for n := uint(5000); n <= 50000; n += 5000 {
		fmt.Printf("%v", n)
		for fp := 0.1; fp >= 0.00001; fp /= 10.0 {
			fmt.Printf("\t%f", fp)
			m, k := estimateParameters(n, fp)
			f := NewWithEstimates(n, fp)
			fp_rate := f.EstimateFalsePositiveRate(n)
			fmt.Printf("\t%v\t%v\t%f", m, k, fp_rate)
		}
		fmt.Println()
	}
}

func BenchmarkSeparateTestAndAdd(b *testing.B) {
	f := NewWithEstimates(uint(b.N), 0.0001)
	key := make([]byte, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint32(key, uint32(i))
		f.Test(key)
		f.Add(key)
	}
}

func BenchmarkCombinedTestAndAdd(b *testing.B) {
	f := NewWithEstimates(uint(b.N), 0.0001)
	key := make([]byte, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint32(key, uint32(i))
		f.TestAndAdd(key)
	}
}

func MergeTest(b *testing.B) {
	f := New(1000, 4)
	n1 := []byte("f")
	f.Add(n1)

	g := New(1000, 4)
	n2 := []byte("g")
	g.Add(n2)

	h := New(999, 4)
	n3 := []byte("h")
	h.Add(n3)

	j := New(1000, 5)
	n4 := []byte("j")
	j.Add(n4)

	var err error

	err = f.Merge(g)
	if err != nil {
		b.Errorf("There should be no error when merging two similar filters")
	}

	err = f.Merge(h)
	if err == nil {
		b.Errorf("There should be an error when merging filters with mismatched m")
	}

	err = f.Merge(j)
	if err == nil {
		b.Errorf("There should be an error when merging filters with mismatched k")
	}

	n2b := f.Test(n2)
	if !n2b {
		b.Errorf("The value doesn't exist after a valid merge")
	}

	n3b := f.Test(n3)
	if n3b {
		b.Errorf("The value exists after an invalid merge")
	}

	n4b := f.Test(n4)
	if n4b {
		b.Errorf("The value exists after an invalid merge")
	}
}
