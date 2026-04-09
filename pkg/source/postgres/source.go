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
	"github.com/foden/cdc/pkg/config"
	"github.com/foden/cdc/pkg/constant"
	"github.com/foden/cdc/pkg/interfaces"
	"github.com/foden/cdc/pkg/models"
	"github.com/foden/cdc/pkg/registry"
	"github.com/foden/cdc/pkg/utils"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

const (
	_outputPlugin        = "pgoutput"
	_protoVersion        = "1"
	_standbyInterval     = 10 * time.Second
	_walLevelLogical     = "logical"
	_snapshotAction      = "NOEXPORT_SNAPSHOT"
	_defaultBackoff      = 30 * time.Second
	_defaultResetBackoff = 1 * time.Second
)

// Internal representation of a row change before JSON encoding
type walTask struct {
	op        string
	namespace string
	table     string
	rel       *pglogrepl.RelationMessage // For WAL
	data      map[string]interface{}     // Pre-processed data (for snapshots)
	old       []*pglogrepl.TupleDataColumn
	new       []*pglogrepl.TupleDataColumn
	lsn       uint64
	ts        int64
}

// Pool for reusing column slices to reduce GC pressure
var columnPool = sync.Pool{
	New: func() interface{} {
		// Pre-allocate space for 32 columns which covers most tables
		return make(map[string]interface{}, 32)
	},
}

func init() {
	registry.RegisterSource(constant.SourceTypePostgres.String(), func(cfg *config.SourceConfig) (interfaces.Source, error) {
		return New(cfg)
	})
}

type PostgresSource struct {
	cfg        *config.SourceConfig
	conn       *pgconn.PgConn
	stop       chan struct{}
	tableMap   map[string]bool
	pipeline   chan<- *models.Event
	flushedLSN uint64

	// Metadata protection
	relMu     sync.RWMutex
	relations map[uint32]*pglogrepl.RelationMessage

	// Single channel for Strict Global Ordering
	taskChan chan *walTask
	wg       sync.WaitGroup
}

func New(cfg *config.SourceConfig) (*PostgresSource, error) {
	tm := make(map[string]bool, len(cfg.Tables))
	for _, t := range cfg.Tables {
		tm[t] = true
	}
	return &PostgresSource{
		cfg:       cfg,
		stop:      make(chan struct{}),
		tableMap:  tm,
		relations: make(map[uint32]*pglogrepl.RelationMessage),
		// Larger buffer to absorb bursts while maintaining sequence
		taskChan: make(chan *walTask, 8192),
	}, nil
}

// InstanceID returns the unique identifier for this source instance.
func (p *PostgresSource) InstanceID() string {
	return p.cfg.InstanceID
}

func (p *PostgresSource) Start(pipeline chan<- *models.Event, ackCh <-chan uint64, initialOffset string) error {
	p.pipeline = pipeline

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

	// Re-enable snapshot if required
	if p.shouldSnapshot(initialOffset) {
		if err := p.runSnapshot(context.Background()); err != nil {
			slog.Error("Initial snapshot failed", "err", err)
			return fmt.Errorf("snapshot failed: %w", err)
		}
	}

	go p.ackLoop(ackCh)
	return p.connectAndStartReplication(pglogrepl.LSN(startLSN))
}

func (p *PostgresSource) processTask(t *walTask) {
	var before, after []byte
	var namespace, table string

	if t.op == "r" {
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
	payload := models.DebeziumPayload{
		Op:     t.op,
		Before: json.RawMessage(before),
		After:  json.RawMessage(after),
		Source: models.SourceMetadata{
			Version:   "1.0",
			Connector: "postgresql",
			Name:      p.cfg.InstanceID,
			TsMs:      t.ts,
			Snapshot:  strconv.FormatBool(t.op == "r"),
			DB:        p.cfg.Database,
			Schema:    namespace,
			Table:     table,
			LSN:       t.lsn,
		},
		TimestampMS: time.Now().UnixMilli(),
	}

	// 3. Fast JSON Marshal
	data, _ := sonic.Marshal(payload)

	topic := p.cfg.Topic
	if topic == "" {
		topic = "cdc"
	}

	subject := fmt.Sprintf("%s.%s.%s.%s", topic, p.cfg.InstanceID, namespace, table)
	p.pipeline <- models.NewEvent(topic, subject, p.cfg.InstanceID, namespace, table, t.op, t.lsn, strconv.FormatUint(t.lsn, 10), data)
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
			obj[name] = p.parseOid(string(col.Data), oid)
		}
	}

	res, _ := sonic.Marshal(obj)
	return res
}

// parseOid maps Postgres types to Go types for proper JSON encoding (numbers vs strings)
func (p *PostgresSource) parseOid(val string, oid uint32) interface{} {
	switch oid {
	case 16: // bool
		return val == "t"
	case 20, 21, 23: // int8, int2, int4
		i, _ := strconv.ParseInt(val, 10, 64)
		return i
	case 700, 701, 1700: // float4, float8, numeric
		f, _ := strconv.ParseFloat(val, 64)
		return f
	case 114, 3802: // json, jsonb
		return json.RawMessage(val)
	default:
		return val
	}
}

// ... (ensureSetup, checkWalLevel, ensurePublication, buildCreatePublicationSQL remain similar but optimized)

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

	// 1. Handle RelationMessage because it needs Write Lock
	if v, ok := msg.(*pglogrepl.RelationMessage); ok {
		p.relMu.Lock()
		p.relations[v.RelationID] = v
		p.relMu.Unlock()
		return
	}

	// 2. Handle other messages with Read Lock
	p.relMu.RLock()
	defer p.relMu.RUnlock()

	switch v := msg.(type) {
	case *pglogrepl.InsertMessage:
		if rel, ok := p.relations[v.RelationID]; ok && p.isTableAllowed(rel.Namespace, rel.RelationName) {
			p.taskChan <- &walTask{op: "c", rel: rel, new: v.Tuple.Columns, lsn: lsn, ts: ts}
		}
	case *pglogrepl.UpdateMessage:
		if rel, ok := p.relations[v.RelationID]; ok && p.isTableAllowed(rel.Namespace, rel.RelationName) {
			p.taskChan <- &walTask{op: "u", rel: rel, old: v.OldTuple.Columns, new: v.NewTuple.Columns, lsn: lsn, ts: ts}
		}
	case *pglogrepl.DeleteMessage:
		if rel, ok := p.relations[v.RelationID]; ok && p.isTableAllowed(rel.Namespace, rel.RelationName) {
			p.taskChan <- &walTask{op: "d", rel: rel, old: v.OldTuple.Columns, lsn: lsn, ts: ts}
		}
	}
}

// ... (Other helper methods: isTableAllowed, Stop, runSnapshot remain largely the same)

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
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		p.cfg.Username, p.cfg.Password, p.cfg.Host, p.cfg.Port, p.cfg.Database)

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

	// Create publication if not exists
	pubName := p.cfg.PublicationName
	var exists bool
	_ = conn.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)", pubName).Scan(&exists)

	if !exists {
		sql := p.buildCreatePublicationSQL(pubName)
		if _, err := conn.Exec(ctx, sql); err != nil {
			return fmt.Errorf("failed to create publication: %w", err)
		}
	}
	return nil
}

func (p *PostgresSource) buildCreatePublicationSQL(pubName string) string {
	quotedPub := utils.QuoteIdent(pubName)
	if len(p.cfg.Tables) == 0 {
		return fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES", quotedPub)
	}
	quoted := make([]string, len(p.cfg.Tables))
	for i, t := range p.cfg.Tables {
		quoted[i] = utils.QuoteIdent(t)
	}
	return fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", quotedPub, strings.Join(quoted, ", "))
}

// shouldSnapshot determines if an initial data export is needed.
func (p *PostgresSource) shouldSnapshot(offset string) bool {
	mode := strings.ToLower(p.cfg.SnapshotMode)
	if mode == "always" {
		return true
	}
	// Default behavior: snapshot if no previous LSN is recorded
	if (mode == "initial" || mode == "") && offset == "" {
		return true
	}
	return false
}

// runSnapshot performs chunked data export using a transaction-scoped cursor.
func (p *PostgresSource) runSnapshot(ctx context.Context) error {
	slog.Info("Starting high-performance snapshot", "instance", p.cfg.InstanceID)

	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		p.cfg.Username, p.cfg.Password, p.cfg.Host, p.cfg.Port, p.cfg.Database)
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	for _, tableName := range p.cfg.Tables {
		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}

		quoted := utils.QuoteIdent(tableName)
		// Declare cursor for chunked fetching (10k rows per fetch for performance)
		_, _ = tx.Exec(ctx, fmt.Sprintf("DECLARE cdc_cursor CURSOR FOR SELECT * FROM %s", quoted))

		for {
			rows, err := tx.Query(ctx, "FETCH 10000 FROM cdc_cursor")
			if err != nil {
				break
			}

			fields := rows.FieldDescriptions()
			count := 0
			for rows.Next() {
				values, _ := rows.Values()
				// Reuse taskChan to leverage existing worker pool for encoding
				// This keeps CPU usage balanced
				p.taskChan <- p.wrapSnapshotRow(tableName, fields, values)
				count++
			}
			rows.Close()
			if count == 0 {
				break
			}
		}
		tx.Commit(ctx)
	}
	return nil
}

// wrapSnapshotRow converts a database row into a walTask for the workers.
func (p *PostgresSource) wrapSnapshotRow(table string, fields []pgconn.FieldDescription, values []interface{}) *walTask {
	data := make(map[string]interface{}, len(fields))
	for i, field := range fields {
		data[field.Name] = values[i]
	}

	// Use schema from config or default to public
	parts := strings.Split(table, ".")
	schema := "public"
	tblName := table
	if len(parts) == 2 {
		schema = parts[0]
		tblName = parts[1]
	}

	return &walTask{
		op:        "r",
		namespace: schema,
		table:     tblName,
		data:      data,
		lsn:       0,
		ts:        time.Now().UnixMilli(),
	}
}

// doConnect establishes the physical replication connection.
func (p *PostgresSource) doConnect(startLSN pglogrepl.LSN) error {
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?replication=database",
		p.cfg.Username, p.cfg.Password, p.cfg.Host, p.cfg.Port, p.cfg.Database)

	conn, err := pgconn.Connect(context.Background(), connStr)
	if err != nil {
		return err
	}
	p.conn = conn

	// Ensure replication slot exists
	_, _ = pglogrepl.CreateReplicationSlot(context.Background(), conn, p.cfg.SlotName, _outputPlugin,
		pglogrepl.CreateReplicationSlotOptions{SnapshotAction: _snapshotAction})

	return pglogrepl.StartReplication(context.Background(), conn, p.cfg.SlotName, startLSN,
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				"proto_version '1'",
				fmt.Sprintf("publication_names '%s'", p.cfg.PublicationName),
			},
		})
}

// readLoopWithReconnect handles automatic failover and backoff.
func (p *PostgresSource) readLoopWithReconnect(startLSN pglogrepl.LSN) {
	backoff := 1 * time.Second
	for {
		select {
		case <-p.stop:
			return
		default:
		}

		errReason := p.readLoop(startLSN)
		slog.Warn("Replication stream interrupted", "reason", errReason, "retry_in", backoff)

		time.Sleep(backoff)

		// Resume from the last acknowledged LSN
		resumeLSN := pglogrepl.LSN(atomic.LoadUint64(&p.flushedLSN))
		if resumeLSN == 0 {
			resumeLSN = startLSN
		}

		if err := p.doConnect(resumeLSN); err == nil {
			backoff = _defaultResetBackoff // Reset on success
			startLSN = resumeLSN
		} else {
			backoff *= 2
			if backoff > _defaultBackoff {
				backoff = _defaultBackoff
			}
		}
	}
}

// ackLoop manages the LSN feedback loop from the downstream pipeline.
func (p *PostgresSource) ackLoop(ackCh <-chan uint64) {
	for lsn := range ackCh {
		current := atomic.LoadUint64(&p.flushedLSN)
		if lsn > current {
			atomic.StoreUint64(&p.flushedLSN, lsn)
		}
	}
}

// isTableAllowed filters tables based on the configuration.
func (p *PostgresSource) isTableAllowed(namespace, table string) bool {
	if len(p.tableMap) == 0 {
		return true
	}
	return p.tableMap[table] || p.tableMap[fmt.Sprintf("%s.%s", namespace, table)]
}

// sendStandbyUpdate sends a WAL position update to PostgreSQL.
// This prevents the server from recycling WAL segments we haven't processed yet.
func (p *PostgresSource) sendStandbyUpdate(clientLSN pglogrepl.LSN) {
	// We report three positions:
	// 1. Write: The LSN we just received from the network.
	// 2. Flush: The LSN that has been successfully acknowledged (ACK) by our pipeline.
	// 3. Apply: Same as Flush in most CDC scenarios.
	flushed := pglogrepl.LSN(atomic.LoadUint64(&p.flushedLSN))

	// If no events have been flushed/ACKed yet, we use the clientLSN
	// to avoid telling Postgres we are at position 0.
	flushPos := flushed
	if flushPos == 0 {
		flushPos = clientLSN
	}

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

// receiveMessage reads a single message from the replication stream with a deadline.
func (p *PostgresSource) receiveMessage(deadline time.Time) (pgproto3.BackendMessage, error) {
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	// Direct call to pgx's connection to pull the next message from the buffer
	return p.conn.ReceiveMessage(ctx)
}
