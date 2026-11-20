package auth

import "testing"

func TestHashAndEqual(t *testing.T) {
	h := Hash("secret")
	if h == "secret" || len(h) != 64 {
		t.Fatalf("unexpected hash %q", h)
	}
	if !Equal(h, Hash("secret")) {
		t.Fatal("equal hashes should match")
	}
	if Equal(h, Hash("other")) {
		t.Fatal("different keys must not match")
	}
}
