package service

import (
	"testing"
	"time"
)

func TestDLQConfirmTokenRoundTrip(t *testing.T) {
	guard := NewDLQReprocessGuard([]byte("test-secret"), time.Minute)
	token, err := guard.Issue(DLQReprocessPlan{
		SelectedIDs: []string{"dlq-1", "dlq-2"},
		Count:       2,
		FilterHash:  "filter-a",
		Now:         1000,
	})
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}

	plan, err := guard.Verify(token, 1010)
	if err != nil {
		t.Fatalf("verify token: %v", err)
	}
	if plan.Count != 2 || plan.FilterHash != "filter-a" {
		t.Fatalf("plan = %+v", plan)
	}
	if len(plan.SelectedIDs) != 2 || plan.SelectedIDs[0] != "dlq-1" || plan.SelectedIDs[1] != "dlq-2" {
		t.Fatalf("selected ids = %+v", plan.SelectedIDs)
	}
}

func TestDLQConfirmTokenRejectsTampering(t *testing.T) {
	guard := NewDLQReprocessGuard([]byte("test-secret"), time.Minute)
	token, err := guard.Issue(DLQReprocessPlan{SelectedIDs: []string{"dlq-1"}, Count: 1, FilterHash: "abc", Now: 1000})
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}

	if _, err := guard.Verify(token+"tampered", 1001); err == nil {
		t.Fatal("verify tampered token succeeded")
	}
}

func TestDLQConfirmTokenExpires(t *testing.T) {
	guard := NewDLQReprocessGuard([]byte("test-secret"), time.Second)
	token, err := guard.Issue(DLQReprocessPlan{SelectedIDs: []string{"dlq-1"}, Count: 1, FilterHash: "abc", Now: 1000})
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}

	if _, err := guard.Verify(token, 1002); err == nil {
		t.Fatal("verify expired token succeeded")
	}
}
