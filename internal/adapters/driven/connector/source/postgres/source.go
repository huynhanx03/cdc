package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bytedance/sonic"
	"github.com/foden/cdc/config"
	sourcecommon "github.com/foden/cdc/internal/adapters/driven/connector/source/common"
	"github.com/foden/cdc/internal/adapters/driven/metrics"
	"github.com/foden/cdc/internal/adapters/driven/registry"
	"github.com/foden/cdc/internal/core/constant"
	"github.com/foden/cdc/internal/core/domain"
	coreflow "github.com/foden/cdc/internal/core/flow"
	"github.com/foden/cdc/internal/core/ports"
	coreruntime "github.com/foden/cdc/internal/core/runtime"
	"github.com/foden/cdc/pkg/retry"
	"github.com/foden/cdc/pkg/utils"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

const (
	_outputPlugin    = "pgoutput"
	_protoVersion    = "1"
	_standbyInterval = 10 * time.Second
	_walLevelLogical = "logical"
	_snapshotAction  = "NOEXPORT_SNAPSHOT"
	_taskChannelSize = 8192
)

// Internal representation of a row change before JSON encoding
type walTask struct {
	op             constant.Op
	namespace      string
	table          string
	rel            *pglogrepl.RelationMessage // For WAL
	data           map[string]interface{}     // Pre-processed data (for snapshots)
	key            string                     // Stable partition key (best effort for snapshots)
	old            []*pglogrepl.TupleDataColumn
	new            []*pglogrepl.TupleDataColumn
	lsn            uint64
	msgID          string
	ts             int64
	partitionCount int
}

// Pool for reusing column slices to reduce GC pressure
var columnPool = sync.Pool{
	New: func() interface{} {
		// Pre-allocate space for 32 columns which covers most tables
		return make(map[string]interface{}, 32)
	},
}

func init() {
	registry.RegisterSource(constant.SourceTypePostgres.String(), func(cfg *ports.SourceConfig) (ports.Source, error) {
		return New(cfg)
	})
}

type PostgresSource struct {
	cfg    *ports.SourceConfig
	conn   *pgconn.PgConn
	stop   chan struct{}
	events chan<- *domain.Event

	flushedLSN uint64

	// Metadata protection
	relMu     sync.RWMutex
	relations map[uint32]*pglogrepl.RelationMessage

	// Single channel for Strict Global Ordering
	taskChan chan *walTask
	wg       sync.WaitGroup

	runtimeRegistry *coreruntime.Registry
	runtimeMetrics  *coreruntime.Metrics
}

func New(cfg *ports.SourceConfig) (*PostgresSource, error) {
	return &PostgresSource{
		cfg:             cfg,
		stop:            make(chan struct{}),
		relations:       make(map[uint32]*pglogrepl.RelationMessage),
		runtimeRegistry: coreruntime.DefaultRegistry(),
		runtimeMetrics:  coreruntime.DefaultMetrics(),
		// Larger buffer to absorb bursts while maintaining sequence
		taskChan: make(chan *walTask, _taskChannelSize),
	}, nil
}

// InstanceID returns the unique identifier for this source instance.
func (p *PostgresSource) InstanceID() string {
	return p.cfg.InstanceID
}

func (p *PostgresSource) Start(events chan<- *domain.Event, ackCh <-chan ports.SourceAck, initialOffset string) error {
	p.events = events

	if err := p.ensureSetup(); err != nil {
		return err
	}

	// Start exactly ONE worker to guarantee 100% global order
	p.wg.Add(1)
	go p.singleOrderedWorker()

	startLSN := uint64(0)
	if initialOffset != "" {
		startLSN, _ = strconv.ParseUint(initialOffset, 10, 64)
	}

	go p.ackLoop(ackCh)
	return p.connectAndStartReplication(pglogrepl.LSN(startLSN))
}

func (p *PostgresSource) processTask(t *walTask) {
	var before, after []byte
	var namespace, table string

	if t.op == constant.OpSnapshot {
		// Snapshot row
		namespace = t.namespace
		table = t.table
		after, _ = sonic.Marshal(t.data)
	} else {
		// WAL row
		namespace = t.rel.Namespace
		table = t.rel.RelationName
		before = p.decodeToJSONRaw(t.rel, t.old)
		after = p.decodeToJSONRaw(t.rel, t.new)
	}

	// 2. Build Debezium-style payload
	payload := domain.DebeziumPayload{
		Op:     t.op,
		Before: json.RawMessage(before),
		After:  json.RawMessage(after),
		Source: domain.SourceMetadata{
			Version:   "1.0",
			Connector: "postgresql",
			Name:      p.cfg.InstanceID,
			TsMs:      t.ts,
			Snapshot:  strconv.FormatBool(t.op == constant.OpSnapshot),
			DB:        p.cfg.Database,
			Schema:    namespace,
			Table:     table,
			LSN:       t.lsn,
		},
		TimestampMS: time.Now().UnixMilli(),
	}

	// 3. Fast JSON Marshal
	data, _ := sonic.Marshal(payload)

	// Topic is auto-derived from "cdc" prefix in the new architecture
	topic := "cdc"

	// 4. Calculate Partition ID based on Primary Key
	partitionID := p.calculatePartition(t)

	// 5. Build Hierarchical Subject (5 levels)
	subject := coreflow.CDCSubject(p.cfg.InstanceID, namespace, table, strconv.Itoa(partitionID))

	offset := ""
	if t.op == constant.OpSnapshot {
		// Snapshot rows do not participate in source resume offsets.
		// Keeping this empty avoids overwriting WAL resume checkpoints with "0".
		offset = ""
	} else {
		offset = strconv.FormatUint(t.lsn, 10)
	}
	ev := sourcecommon.BuildEvent(
		topic,
		subject,
		p.cfg.InstanceID,
		namespace,
		table,
		t.op,
		t.lsn,
		offset,
		data,
		partitionID,
		t.msgID,
	)

	p.events <- ev
	if p.runtimeMetrics != nil {
		p.runtimeMetrics.RecordSourceProduced(p.cfg.InstanceID, namespace, table, 1, t.ts)
	}
	metrics.EventsProducedTotal.WithLabelValues(p.cfg.InstanceID, metrics.StatusSuccess).Inc()
}

// calculatePartition hashes the Primary Key of the row to determine the destination partition.
func (p *PostgresSource) calculatePartition(t *walTask) int {
	partitionCount := t.partitionCount
	if partitionCount <= 0 {
		partitionCount = 4
	}

	// Snapshot rows use a map, WAL tasks use pglogrepl.TupleDataColumn
	var pkValues []string

	if t.op == constant.OpSnapshot {
		// Snapshot rows prefer a stable row key; table hashing keeps distribution useful
		// when the row key is not available.
		if t.key != "" {
			return utils.GeneratePartition(t.key, partitionCount)
		}
		return utils.GeneratePartition(fmt.Sprintf("%s.%s", t.namespace, t.table), partitionCount)
	}

	// For WAL tasks, check RelationMessage for identity columns
	for i, col := range t.rel.Columns {
		// Key columns have Flag bit 0 set (1)
		if col.Flags&1 != 0 {
			var val string
			// Use the new data for Insert/Update, or old data for Delete
			targetCols := t.new
			if t.op == constant.OpDelete {
				targetCols = t.old
			}

			if i < len(targetCols) && targetCols[i].DataType == 't' {
				val = string(targetCols[i].Data)
				pkValues = append(pkValues, val)
			}
		}
	}

	if len(pkValues) == 0 {
		return 0
	}

	// Hash the combined PK values using the configured partition count
	return utils.GeneratePartition(utils.CombineKeys(pkValues...), partitionCount)
}

// decodeToJSONRaw converts raw WAL bytes to a format that json.Marshal understands
func (p *PostgresSource) decodeToJSONRaw(rel *pglogrepl.RelationMessage, cols []*pglogrepl.TupleDataColumn) json.RawMessage {
	if cols == nil {
		return nil
	}

	// Reuse map from pool to avoid allocation
	obj := columnPool.Get().(map[string]interface{})
	defer func() {
		// Clean the map before putting it back
		for k := range obj {
			delete(obj, k)
		}
		columnPool.Put(obj)
	}()

	for i, col := range cols {
		if i >= len(rel.Columns) || col.DataType == 'u' {
			continue // Skip unchanged TOAST or overflow
		}

		name := rel.Columns[i].Name
		oid := rel.Columns[i].DataType

		switch col.DataType {
		case 'n': // Null
			obj[name] = nil
		case 't': // Text formatted value
			obj[name] = p.parseOid(col.Data, oid)
		}
	}

	res, _ := sonic.Marshal(obj)
	return res
}

// parseOid maps Postgres types to Go types for proper JSON encoding (numbers vs strings)
func (p *PostgresSource) parseOid(val []byte, oid uint32) interface{} {
	switch oid {
	case 16: // bool
		return len(val) == 1 && val[0] == 't'
	case 20, 21, 23: // int8, int2, int4
		i, err := strconv.ParseInt(string(val), 10, 64)
		if err != nil {
			return string(val)
		}
		return i
	case 700, 701: // float4, float8
		f, err := strconv.ParseFloat(string(val), 64)
		if err != nil {
			return string(val)
		}
		return f
	case 1700: // numeric
		return string(val)
	case 114, 3802: // json, jsonb
		return json.RawMessage(val)
	default:
		return string(val)
	}
}

func (p *PostgresSource) connectAndStartReplication(startLSN pglogrepl.LSN) error {
	if err := p.doConnect(startLSN); err != nil {
		return err
	}
	// Launch the optimized read loop
	go p.readLoopWithReconnect(startLSN)
	return nil
}

func (p *PostgresSource) readLoop(startLSN pglogrepl.LSN) string {
	clientLSN := startLSN
	nextStandby := time.Now().Add(_standbyInterval)

	for {
		select {
		case <-p.stop:
			return "stopped"
		default:
		}

		if time.Now().After(nextStandby) {
			p.sendStandbyUpdate(clientLSN)
			nextStandby = time.Now().Add(_standbyInterval)
		}

		// Set a read deadline to prevent hanging forever
		rawMsg, err := p.receiveMessage(nextStandby)
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			return fmt.Sprintf("receive error: %v", err)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			continue
		}

		// Optimized handleCopyData
		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, _ := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if pkm.ServerWALEnd > clientLSN {
				clientLSN = pkm.ServerWALEnd
			}
			if pkm.ReplyRequested {
				nextStandby = time.Time{}
			}
		case pglogrepl.XLogDataByteID:
			xld, _ := pglogrepl.ParseXLogData(msg.Data[1:])
			logicalMsg, _ := pglogrepl.Parse(xld.WALData)

			lsn := uint64(xld.WALStart + pglogrepl.LSN(len(xld.WALData)))
			p.dispatchToWorkers(logicalMsg, lsn)
			clientLSN = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
		}
	}
}

func (p *PostgresSource) singleOrderedWorker() {
	defer p.wg.Done()
	for task := range p.taskChan {
		p.processTask(task)
	}
}

// dispatchToWorkers pushes the task to the worker pool channel
func (p *PostgresSource) dispatchToWorkers(msg pglogrepl.Message, lsn uint64) {
	ts := time.Now().UnixMilli()
	changeIdx := 0

	// 1. Handle RelationMessage because it needs Write Lock
	if v, ok := msg.(*pglogrepl.RelationMessage); ok {
		p.relMu.Lock()
		p.relations[v.RelationID] = v
		p.relMu.Unlock()
		return
	}

	switch v := msg.(type) {
	case *pglogrepl.InsertMessage:
		p.relMu.RLock()
		rel, ok := p.relations[v.RelationID]
		p.relMu.RUnlock()
		if !ok {
			return
		}
		interest, active := p.runtimeRegistry.LookupTable(p.cfg.InstanceID, rel.Namespace, rel.RelationName)
		if !active {
			return // No flow needs this table
		}
		msgID := p.walMessageID(lsn, v.RelationID, changeIdx, rel, constant.OpCreate)
		changeIdx++
		p.taskChan <- &walTask{op: constant.OpCreate, rel: rel, new: v.Tuple.Columns, lsn: lsn, msgID: msgID, ts: ts, partitionCount: int(interest.PartitionCount)}
	case *pglogrepl.UpdateMessage:
		p.relMu.RLock()
		rel, ok := p.relations[v.RelationID]
		p.relMu.RUnlock()
		if !ok {
			return
		}
		interest, active := p.runtimeRegistry.LookupTable(p.cfg.InstanceID, rel.Namespace, rel.RelationName)
		if !active {
			return
		}
		msgID := p.walMessageID(lsn, v.RelationID, changeIdx, rel, constant.OpUpdate)
		changeIdx++
		p.taskChan <- &walTask{op: constant.OpUpdate, rel: rel, old: tupleColumns(v.OldTuple), new: tupleColumns(v.NewTuple), lsn: lsn, msgID: msgID, ts: ts, partitionCount: int(interest.PartitionCount)}
	case *pglogrepl.DeleteMessage:
		p.relMu.RLock()
		rel, ok := p.relations[v.RelationID]
		p.relMu.RUnlock()
		if !ok {
			return
		}
		interest, active := p.runtimeRegistry.LookupTable(p.cfg.InstanceID, rel.Namespace, rel.RelationName)
		if !active {
			return
		}
		msgID := p.walMessageID(lsn, v.RelationID, changeIdx, rel, constant.OpDelete)
		changeIdx++
		p.taskChan <- &walTask{op: constant.OpDelete, rel: rel, old: tupleColumns(v.OldTuple), lsn: lsn, msgID: msgID, ts: ts, partitionCount: int(interest.PartitionCount)}
	}
}

func tupleColumns(tuple *pglogrepl.TupleData) []*pglogrepl.TupleDataColumn {
	if tuple == nil {
		return nil
	}
	return tuple.Columns
}

func (p *PostgresSource) walMessageID(lsn uint64, relationID uint32, changeIdx int, rel *pglogrepl.RelationMessage, op constant.Op) string {
	return fmt.Sprintf(
		"%s-postgres-%d-%d-%d-%s.%s-%s",
		p.cfg.InstanceID,
		lsn,
		relationID,
		changeIdx,
		rel.Namespace,
		rel.RelationName,
		string(op),
	)
}

func (p *PostgresSource) Stop() error {
	slog.Info("stopping postgres source")
	close(p.stop)
	close(p.taskChan) // Signal workers to stop
	p.wg.Wait()       // Wait for all encoding tasks to finish

	if p.conn != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return p.conn.Close(ctx)
	}
	return nil
}

// ensureSetup validates the environment and prepares the publication.
func (p *PostgresSource) ensureSetup() error {
	connStr := config.PostgresDSN(p.cfg.Host, p.cfg.Port, p.cfg.Username, p.cfg.Password, p.cfg.Database)

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return fmt.Errorf("setup connection failed: %w", err)
	}
	defer conn.Close(ctx)

	// Verify wal_level is logical
	var walLevel string
	if err := conn.QueryRow(ctx, "SHOW wal_level").Scan(&walLevel); err != nil {
		return fmt.Errorf("failed to check wal_level: %w", err)
	}
	if walLevel != _walLevelLogical {
		return fmt.Errorf("wal_level must be 'logical', current: %s", walLevel)
	}

	return nil
}

// doConnect establishes the physical replication connection.
func (p *PostgresSource) doConnect(startLSN pglogrepl.LSN) error {
	connStr := config.PostgresDSN(p.cfg.Host, p.cfg.Port, p.cfg.Username, p.cfg.Password, p.cfg.Database) + "?replication=database"

	conn, err := pgconn.Connect(context.Background(), connStr)
	if err != nil {
		return err
	}
	p.conn = conn

	// Ensure replication slot exists
	slotName := p.slotName()
	_, _ = pglogrepl.CreateReplicationSlot(context.Background(), conn, slotName, _outputPlugin,
		pglogrepl.CreateReplicationSlotOptions{SnapshotAction: _snapshotAction})

	return pglogrepl.StartReplication(context.Background(), conn, slotName, startLSN,
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				"proto_version '1'",
				fmt.Sprintf("publication_names '%s'", p.publicationName()),
			},
		})
}

func (p *PostgresSource) publicationName() string {
	return "cdc_pub_" + postgresIdentifierSuffix(p.cfg.InstanceID)
}

func (p *PostgresSource) slotName() string {
	return "cdc_slot_" + postgresIdentifierSuffix(p.cfg.InstanceID)
}

func postgresIdentifierSuffix(instanceID string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(instanceID) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
			continue
		}
		b.WriteByte('_')
	}
	suffix := strings.Trim(b.String(), "_")
	if suffix == "" {
		return "source"
	}
	return suffix
}

// readLoopWithReconnect handles automatic failover and backoff.
func (p *PostgresSource) readLoopWithReconnect(startLSN pglogrepl.LSN) {
	for {
		select {
		case <-p.stop:
			return
		default:
		}

		errReason := p.readLoop(startLSN)
		slog.Warn("Replication stream interrupted", "reason", errReason)

		// Use retry for reconnection with exponential backoff
		err := retry.Do(context.Background(), retry.SourceReconnectConfig(), func() error {
			select {
			case <-p.stop:
				return nil // not an error, just stopping
			default:
			}
			resumeLSN := pglogrepl.LSN(atomic.LoadUint64(&p.flushedLSN))
			if resumeLSN == 0 {
				resumeLSN = startLSN
			}
			if err := p.doConnect(resumeLSN); err != nil {
				return err
			}
			startLSN = resumeLSN
			return nil
		})

		if err != nil {
			slog.Error("source reconnect failed permanently", "err", err)
			return
		}
	}
}

// ackLoop manages the LSN feedback loop from the downstream flow.
func (p *PostgresSource) ackLoop(ackCh <-chan ports.SourceAck) {
	for ack := range ackCh {
		if ack.LSN == 0 {
			continue
		}
		current := atomic.LoadUint64(&p.flushedLSN)
		if ack.LSN > current {
			atomic.StoreUint64(&p.flushedLSN, ack.LSN)
		}
	}
}

// sendStandbyUpdate sends a WAL position update to PostgreSQL.
// This prevents the server from recycling WAL segments we haven't processed yet.
func (p *PostgresSource) sendStandbyUpdate(clientLSN pglogrepl.LSN) {
	// We report three positions:
	// 1. Write: The LSN we just received from the network.
	// 2. Flush: The LSN that has been successfully acknowledged (ACK) by our flow.
	// 3. Apply: Same as Flush in most CDC scenarios.
	flushed := pglogrepl.LSN(atomic.LoadUint64(&p.flushedLSN))

	flushPos := standbyFlushPosition(flushed, clientLSN)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := pglogrepl.SendStandbyStatusUpdate(ctx, p.conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: clientLSN,
		WALFlushPosition: flushPos,
		WALApplyPosition: flushPos,
		ReplyRequested:   false,
	})

	if err != nil {
		slog.Error("Failed to send standby status update", "err", err, "instance", p.cfg.InstanceID)
	}
}

func standbyFlushPosition(flushed, _ pglogrepl.LSN) pglogrepl.LSN {
	return flushed
}

// receiveMessage reads a single message from the replication stream with a deadline.
func (p *PostgresSource) receiveMessage(deadline time.Time) (pgproto3.BackendMessage, error) {
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	// Direct call to pgx's connection to pull the next message from the buffer
	return p.conn.ReceiveMessage(ctx)
}
