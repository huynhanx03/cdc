package flow

import "testing"

func TestCDCSubjectEscapesDotsAndWildcards(t *testing.T) {
	subj := CDCSubject("src.1", "public", "order.items", "0")
	if subj == "cdc.src.1.public.order.items.0" {
		t.Fatalf("subject was not encoded: %s", subj)
	}
	if got := CDCFilterSubject("src.1", "public", "order.items"); got == "cdc.src.1.public.order.items.*" {
		t.Fatalf("filter subject was not encoded: %s", got)
	}
}
