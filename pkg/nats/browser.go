package nats

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/foden/cdc/pkg/models"
	"github.com/nats-io/nats.go/jetstream"
)

// ListMessages fetches messages from the stream using an Ephemeral Consumer for high performance.
// It supports pagination and filtering by message status (Sent/Unsent), topic, or partition.
func (c *Client) ListMessages(ctx context.Context, status models.MessageStatus, limit int, page int, topic string, partition string) ([]*MessageItem, uint64, error) {
	stream, err := c.js.Stream(ctx, c.streamName)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get stream: %w", err)
	}

	info, _ := stream.Info(ctx)
	total := info.State.Msgs
	if total == 0 {
		return []*MessageItem{}, 0, nil
	}

	// 1. Define the Filter Subject based on topic or specific partition
	filter := ">" // Default: match all subjects
	if partition != "" {
		filter = partition
	} else if topic != "" {
		filter = fmt.Sprintf("cdc.%s.>", topic)
	}

	// 2. Determine the starting sequence (Offset)
	startSeq := info.State.FirstSeq
	ackFloor, _, err := c.GetConsumerInfo(ctx, "pipeline-worker")

	// If looking for Unsent messages, start from the last acknowledged message + 1
	if status == models.MessageStatusUnsent && err == nil {
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

// ListTopics returns unique topic names extracted from the stream's subjects with pagination.
func (c *Client) ListTopics(ctx context.Context, limit int, page int) ([]string, uint64, error) {
	info, err := c.GetStreamInfo(ctx)
	if err != nil {
		return nil, 0, err
	}

	// Use a map to identify unique topics
	topicMap := make(map[string]struct{})
	for _, subj := range info.Config.Subjects {
		if t := c.extractTopic(subj); t != "" {
			topicMap[t] = struct{}{}
		}
	}

	topics := make([]string, 0, len(topicMap))
	for t := range topicMap {
		topics = append(topics, t)
	}

	return paginate(topics, limit, page), uint64(len(topics)), nil
}

// ListPartitions returns all subjects matching a specific topic prefix with pagination.
func (c *Client) ListPartitions(ctx context.Context, topic string, limit int, page int) ([]string, uint64, error) {
	info, err := c.GetStreamInfo(ctx)
	if err != nil {
		return nil, 0, err
	}

	prefix := "cdc."
	if topic != "" {
		prefix = fmt.Sprintf("cdc.%s.", topic)
	}

	var partitions []string
	for _, subject := range info.Config.Subjects {
		if strings.HasPrefix(subject, prefix) {
			partitions = append(partitions, subject)
		}
	}

	return paginate(partitions, limit, page), uint64(len(partitions)), nil
}

// extractTopic parses the topic name from subjects like "cdc.users.updates" -> "users"
func (c *Client) extractTopic(subject string) string {
	// Use SplitN to limit memory allocations
	parts := strings.SplitN(subject, ".", 3)
	if len(parts) >= 2 && parts[0] == "cdc" {
		return parts[1]
	}
	return ""
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
