package bloom

import (
	"encoding/binary"
	"fmt"
	"testing"
)

func TestBasic(t *testing.T) {
	f := New(1000, 4)
	n1 := []byte("Bess")
	n2 := []byte("Jane")
	f.Add(n1)
	n1b := f.Test(n1)
	n2b := f.Test(n2)
	if !n1b {
		t.Errorf("%v should be in.", n1)
	}
	if n2b {
		t.Errorf("%v should not be in.", n2)
	}
}

func TestBasicUint32(t *testing.T) {
	f := New(1000, 4)
	n1 := make([]byte, 4)
	n2 := make([]byte, 4)
	n3 := make([]byte, 4)
	binary.BigEndian.PutUint32(n1, 100)
	binary.BigEndian.PutUint32(n2, 101)
	binary.BigEndian.PutUint32(n3, 102)
	f.Add(n1)
	n1b := f.Test(n1)
	n2b := f.Test(n2)
	f.Test(n3)
	if !n1b {
		t.Errorf("%v should be in.", n1)
	}
	if n2b {
		t.Errorf("%v should not be in.", n2)
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

func BenchmarkDirect(t *testing.B) {
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

func BenchmarkEstimted(t *testing.B) {
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
