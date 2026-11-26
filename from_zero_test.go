package bloom

import "testing"

// New clamps m and k to at least one so that the location computation never
// divides by zero. From and FromWithM build the same structure and must apply
// the same clamp.

func TestFromEmptyData(t *testing.T) {
	f := From(nil, 3)
	if f.Cap() != 1 {
		t.Fatalf("Cap() = %d, want 1", f.Cap())
	}
	if f.Test([]byte("x")) {
		t.Fatalf("empty filter reported a member")
	}
	f.Add([]byte("x"))
	if !f.Test([]byte("x")) {
		t.Fatalf("filter lost a member after Add")
	}
}

func TestFromWithZeroM(t *testing.T) {
	f := FromWithM(nil, 0, 3)
	if f.Cap() != 1 {
		t.Fatalf("Cap() = %d, want 1", f.Cap())
	}
	if f.Test([]byte("x")) {
		t.Fatalf("empty filter reported a member")
	}
}

func TestFromWithZeroK(t *testing.T) {
	f := FromWithM([]uint64{0, 0}, 128, 0)
	if f.K() != 1 {
		t.Fatalf("K() = %d, want 1", f.K())
	}
	if f.Test([]byte("x")) {
		t.Fatalf("empty filter reported a member")
	}
}
