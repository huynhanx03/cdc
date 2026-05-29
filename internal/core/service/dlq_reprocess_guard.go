package service

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type DLQReprocessPlan struct {
	SelectedIDs []string `json:"selected_ids,omitempty"`
	Count       uint32   `json:"count"`
	FilterHash  string   `json:"filter_hash,omitempty"`
	Now         int64    `json:"now"`
}

type DLQReprocessGuard struct {
	secret []byte
	ttl    time.Duration
}

func NewDLQReprocessGuard(secret []byte, ttl time.Duration) *DLQReprocessGuard {
	if len(secret) == 0 {
		secret = []byte("cdc-dlq-reprocess")
	}
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return &DLQReprocessGuard{
		secret: append([]byte(nil), secret...),
		ttl:    ttl,
	}
}

func (g *DLQReprocessGuard) Issue(plan DLQReprocessPlan) (string, error) {
	if plan.Now <= 0 {
		plan.Now = time.Now().Unix()
	}
	payload, err := json.Marshal(plan)
	if err != nil {
		return "", err
	}
	signature := g.sign(payload)
	return base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

func (g *DLQReprocessGuard) Verify(token string, now int64) (DLQReprocessPlan, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return DLQReprocessPlan{}, fmt.Errorf("invalid confirm token")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return DLQReprocessPlan{}, fmt.Errorf("invalid confirm token payload: %w", err)
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return DLQReprocessPlan{}, fmt.Errorf("invalid confirm token signature: %w", err)
	}
	if !hmac.Equal(signature, g.sign(payload)) {
		return DLQReprocessPlan{}, fmt.Errorf("invalid confirm token signature")
	}
	var plan DLQReprocessPlan
	if err := json.Unmarshal(payload, &plan); err != nil {
		return DLQReprocessPlan{}, err
	}
	if now <= 0 {
		now = time.Now().Unix()
	}
	if now-plan.Now > int64(g.ttl.Seconds()) {
		return DLQReprocessPlan{}, fmt.Errorf("confirm token expired")
	}
	return plan, nil
}

func (g *DLQReprocessGuard) sign(payload []byte) []byte {
	mac := hmac.New(sha256.New, g.secret)
	_, _ = mac.Write(payload)
	return mac.Sum(nil)
}
