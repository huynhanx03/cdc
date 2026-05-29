package nats

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/foden/cdc/internal/core/domain"
	"github.com/foden/cdc/internal/core/ports"
	"github.com/nats-io/nats.go/jetstream"
)

// ListMessages fetches messages from the stream using an Ephemeral Consumer for high performance.
// It supports pagination and filtering by message status (Sent/Unsent), topic, or partition.
func (c *Client) ListMessages(ctx context.Context, status domain.MessageStatus, limit int, page int, topic string, partition string) ([]*MessageItem, uint64, error) {
	stream, err := c.js.Stream(ctx, c.streamName)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get stream: %w", err)
	}

	info, _ := stream.Info(ctx)
	total := info.State.Msgs
	if total == 0 {
		return []*MessageItem{}, 0, nil
	}

	// 1. Define the Filter Subject based on topic or specific partition.
	filter := ">" // Default: match all subjects
	if partition != "" {
		if topic != "" {
			filter = topicPartitionSubject(topic, partition)
		} else {
			filter = partition
		}
	} else if topic != "" {
		if strings.HasPrefix(topic, "cdc.") {
			filter = topic
			if !strings.HasSuffix(filter, ".>") {
				filter = strings.TrimSuffix(filter, ".") + ".>"
			}
		} else {
			filter = fmt.Sprintf("cdc.%s.>", topic)
		}
	}

	// 2. Determine the starting sequence (Offset)
	startSeq := info.State.FirstSeq
	ackFloor, _, err := c.GetConsumerInfo(ctx, "")

	// If looking for Unsent messages, start from the last acknowledged message + 1
	if status == domain.MessageStatusUnsent && err == nil {
		if ackFloor >= info.State.LastSeq {
			return []*MessageItem{}, total, nil // All messages processed
		}
		startSeq = ackFloor + 1
	}

	// 3. Calculate pagination offset (skip)
	skip := 0
	if page > 1 && limit > 0 {
		skip = (page - 1) * limit
	}

	// 4. Create an Ephemeral Consumer to let NATS handle server-side filtering
	cons, err := c.js.CreateConsumer(ctx, c.streamName, jetstream.ConsumerConfig{
		FilterSubject:     filter,
		DeliverPolicy:     jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:       startSeq,
		AckPolicy:         jetstream.AckNonePolicy, // View-only, no acknowledgement needed
		InactiveThreshold: 10 * time.Second,        // Auto-cleanup after 10s of inactivity
	})
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create ephemeral consumer: %w", err)
	}

	// 5. Fetch messages in a batch (skip + limit)
	// Guard rail: prevent fetching excessively large batches
	fetchCount := limit + skip
	if fetchCount > 200 {
		fetchCount = 200
	}

	iter, err := cons.Fetch(fetchCount, jetstream.FetchMaxWait(1*time.Second))
	if err != nil {
		return nil, 0, err
	}

	var result []*MessageItem
	count := 0
	for msg := range iter.Messages() {
		count++
		// Skip records belonging to previous pages
		if count <= skip {
			continue
		}

		meta, _ := msg.Metadata()

		// Map NATS headers to internal map
		headers := make(map[string]string)
		msgHdr := msg.Headers()
		for k := range msgHdr {
			headers[k] = msgHdr.Get(k)
		}

		result = append(result, &MessageItem{
			Sequence:  meta.Sequence.Stream,
			Timestamp: meta.Timestamp.UnixMilli(),
			Subject:   msg.Subject(),
			Data:      msg.Data(),
			Headers:   headers,
		})

		if len(result) >= limit {
			break
		}
	}

	return result, total, nil
}

// ListMessagesWithFilter applies the richer Explorer filter model. JetStream
// subject filtering is still pushed down to NATS; metadata and payload
// predicates are applied in-process over a capped fetch window.
func (c *Client) ListMessagesWithFilter(ctx context.Context, status domain.MessageStatus, limit int, page int, filter ports.NATSMessageFilter) ([]*MessageItem, uint64, error) {
	stream, err := c.js.Stream(ctx, c.streamName)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get stream: %w", err)
	}

	info, err := stream.Info(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get stream info: %w", err)
	}
	if info.State.Msgs == 0 {
		return []*MessageItem{}, 0, nil
	}

	if limit <= 0 {
		limit = 25
	}
	if page <= 0 {
		page = 1
	}

	startSeq := info.State.FirstSeq
	ackFloor, _, err := c.GetConsumerInfo(ctx, "")
	if status == domain.MessageStatusUnsent && err == nil {
		if ackFloor >= info.State.LastSeq {
			return []*MessageItem{}, 0, nil
		}
		startSeq = ackFloor + 1
	}

	consumer, err := c.js.CreateConsumer(ctx, c.streamName, jetstream.ConsumerConfig{
		FilterSubject:     ExplorerMessageFilter(filter).NATSFilterSubject(),
		DeliverPolicy:     jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:       startSeq,
		AckPolicy:         jetstream.AckNonePolicy,
		InactiveThreshold: 10 * time.Second,
	})
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create ephemeral consumer: %w", err)
	}

	fetchCount := limit * page * 4
	if fetchCount < 100 {
		fetchCount = 100
	}
	if fetchCount > 500 {
		fetchCount = 500
	}

	iter, err := consumer.Fetch(fetchCount, jetstream.FetchMaxWait(time.Second))
	if err != nil {
		return nil, 0, err
	}

	matches := make([]*MessageItem, 0, fetchCount)
	for msg := range iter.Messages() {
		item := messageItemFromJetStreamMsg(msg)
		if !ExplorerMessageFilter(filter).Matches(item) {
			continue
		}
		matches = append(matches, item)
	}
	sort.SliceStable(matches, func(i, j int) bool {
		if filter.Sort == "newest" {
			return matches[i].Sequence > matches[j].Sequence
		}
		return matches[i].Sequence < matches[j].Sequence
	})
	skipMatches := (page - 1) * limit
	if skipMatches >= len(matches) {
		return []*MessageItem{}, uint64(len(matches)), nil
	}
	end := skipMatches + limit
	if end > len(matches) {
		end = len(matches)
	}
	return matches[skipMatches:end], uint64(len(matches)), nil
}

func (c *Client) ListDLQMessages(ctx context.Context, limit int, page int) ([]*MessageItem, uint64, error) {
	messages, total, err := c.listStreamMessages(ctx, c.dlqStreamName(), "dlq.>", limit, page)
	if err != nil {
		return nil, 0, err
	}
	return messages, total, nil
}

// ListTopics returns unique topic names extracted from the stream's subjects with pagination.
func (c *Client) ListTopics(ctx context.Context, limit int, page int) ([]string, uint64, error) {
	subjects, err := c.listRecentSubjects(ctx, c.streamName, "cdc.>", 500)
	if err != nil {
		return nil, 0, err
	}
	// Group subjects by the first 4 segments (cdc.inst.schema.table) to avoid partition redundancy in UI
	uniqueTopics := make(map[string]bool)
	var topics []string
	for _, s := range subjects {
		parts := strings.Split(s, ".")
		if len(parts) >= 4 {
			topic := strings.Join(parts[:4], ".")
			if !uniqueTopics[topic] {
				uniqueTopics[topic] = true
				topics = append(topics, topic)
			}
		} else {
			// Keep short subjects visible as standalone topics.
			if !uniqueTopics[s] {
				uniqueTopics[s] = true
				topics = append(topics, s)
			}
		}
	}

	return paginate(topics, limit, page), uint64(len(topics)), nil
}

// ListPartitions returns all subjects matching a specific topic prefix with pagination.
func (c *Client) ListPartitions(ctx context.Context, topic string, limit int, page int) ([]string, uint64, error) {
	subjects, err := c.listRecentSubjects(ctx, c.streamName, "cdc.>", 500)
	if err != nil {
		return nil, 0, err
	}

	prefix := "cdc."
	if topic != "" {
		if strings.HasPrefix(topic, "cdc.") {
			prefix = topic
			if !strings.HasSuffix(prefix, ".") {
				prefix += "."
			}
		} else {
			prefix = fmt.Sprintf("cdc.%s.", topic)
		}
	}

	uniquePartitions := make(map[string]bool)
	var partitions []string
	for _, subject := range subjects {
		if strings.HasPrefix(subject, prefix) {
			partition := strings.TrimPrefix(subject, prefix)
			if partition == "" || uniquePartitions[partition] {
				continue
			}
			uniquePartitions[partition] = true
			partitions = append(partitions, partition)
		}
	}

	return paginate(partitions, limit, page), uint64(len(partitions)), nil
}

func topicPartitionSubject(topic string, partition string) string {
	if strings.HasPrefix(partition, "cdc.") {
		return partition
	}
	if strings.HasPrefix(topic, "cdc.") {
		return strings.TrimSuffix(topic, ".") + "." + strings.TrimPrefix(partition, ".")
	}
	return fmt.Sprintf("cdc.%s.%s", strings.TrimSuffix(topic, "."), strings.TrimPrefix(partition, "."))
}

func (c *Client) listRecentSubjects(ctx context.Context, streamName string, filter string, max int) ([]string, error) {
	messages, _, err := c.listStreamMessages(ctx, streamName, filter, max, 1)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool, len(messages))
	subjects := make([]string, 0, len(messages))
	for _, message := range messages {
		if seen[message.Subject] {
			continue
		}
		seen[message.Subject] = true
		subjects = append(subjects, message.Subject)
	}
	return subjects, nil
}

func (c *Client) listStreamMessages(ctx context.Context, streamName string, filter string, limit int, page int) ([]*MessageItem, uint64, error) {
	stream, err := c.js.Stream(ctx, streamName)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get stream %s: %w", streamName, err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get stream info %s: %w", streamName, err)
	}
	total := info.State.Msgs
	if total == 0 {
		return []*MessageItem{}, 0, nil
	}

	if limit <= 0 {
		limit = 25
	}
	if page <= 0 {
		page = 1
	}
	skip := (page - 1) * limit
	fetchCount := limit + skip
	if fetchCount > 500 {
		fetchCount = 500
	}

	consumer, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		FilterSubject:     filter,
		DeliverPolicy:     jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:       info.State.FirstSeq,
		AckPolicy:         jetstream.AckNonePolicy,
		InactiveThreshold: 10 * time.Second,
	})
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create ephemeral consumer: %w", err)
	}

	iter, err := consumer.Fetch(fetchCount, jetstream.FetchMaxWait(time.Second))
	if err != nil {
		return nil, 0, err
	}

	result := make([]*MessageItem, 0, limit)
	count := 0
	for msg := range iter.Messages() {
		count++
		if count <= skip {
			continue
		}
		meta, _ := msg.Metadata()
		headers := make(map[string]string)
		for key := range msg.Headers() {
			headers[key] = msg.Headers().Get(key)
		}
		result = append(result, &MessageItem{
			Sequence:  meta.Sequence.Stream,
			Timestamp: meta.Timestamp.UnixMilli(),
			Subject:   msg.Subject(),
			Data:      msg.Data(),
			Headers:   headers,
		})
		if len(result) >= limit {
			break
		}
	}
	return result, total, nil
}

func messageItemFromJetStreamMsg(msg jetstream.Msg) *MessageItem {
	meta, _ := msg.Metadata()
	headers := make(map[string]string)
	msgHdr := msg.Headers()
	for key := range msgHdr {
		headers[key] = msgHdr.Get(key)
	}
	item := &MessageItem{
		Subject: msg.Subject(),
		Data:    msg.Data(),
		Headers: headers,
	}
	if meta != nil {
		item.Sequence = meta.Sequence.Stream
		item.Timestamp = meta.Timestamp.UnixMilli()
	}
	return item
}

// paginate handles slicing of string arrays based on limit and page parameters
func paginate(items []string, limit, page int) []string {
	if limit <= 0 || page <= 0 {
		return items
	}

	total := len(items)
	start := (page - 1) * limit

	if start >= total {
		return []string{}
	}

	end := start + limit
	if end > total {
		end = total
	}

	return items[start:end]
}
