package xdsresource

import (
	"testing"
)

func TestParseName_ProcessingDirective(t *testing.T) {
	in := "xdstp://auth/type/id#hash"
	want := &Name{
		Scheme:              "xdstp",
		Authority:           "auth",
		Type:                "type",
		ID:                  "id",
		processingDirective: "hash",
	}
	got := ParseName(in)
	// We cannot access unexported field processingDirective directly in cmp.Equal if we don't allow it.
	// But we are in the same package, so we can access it?
	// Wait, Name struct definition has processingDirective as unexported?
	// Yes: processingDirective string
	// So cmp.Equal might complain if we try to compare it without AllowUnexported.
	// But TestParseName uses IgnoreFields.
	
	if got.processingDirective != want.processingDirective {
		t.Errorf("processingDirective = %q, want %q", got.processingDirective, want.processingDirective)
	}
	if got.String() != in {
		t.Errorf("String() = %q, want %q", got.String(), in)
	}
}
