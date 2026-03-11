package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/foden/cdc/pkg/config"
	"github.com/foden/cdc/pkg/constant"
	"github.com/foden/cdc/pkg/interfaces"
	"github.com/foden/cdc/pkg/models"
	"github.com/foden/cdc/pkg/registry"
)

const (
	outputPlugin    = "pgoutput"
	protoVersion    = "1"
	standbyInterval = 10 * time.Second
	walLevelLogical = "logical"
	snapshotAction  = "NOEXPORT_SNAPSHOT"
)

func init() {
	registry.RegisterSource(constant.SourceTypePostgres.String(), func(cfg *config.SourceConfig) (interfaces.Source, error) {
		return New(cfg)
	})
}

// PostgresSource streams CDC events via PostgreSQL logical replication.
type PostgresSource struct {
	cfg      *config.SourceConfig
	conn     *pgconn.PgConn
	stop       chan struct{}
	tableMap   map[string]bool
	pipeline   chan<- *models.Event
	flushedLSN uint64
}

// New creates a PostgresSource. Connection is deferred to Start().
func New(cfg *config.SourceConfig) (*PostgresSource, error) {
	tm := make(map[string]bool, len(cfg.Tables))
	for _, t := range cfg.Tables {
		tm[t] = true
	}
	return &PostgresSource{
		cfg:      cfg,
		stop:     make(chan struct{}),
		tableMap: tm,
	}, nil
}

// Start performs auto-setup, connects via replication protocol, and streams events.
func (p *PostgresSource) Start(pipeline chan<- *models.Event, ackCh <-chan uint64) error {
	p.pipeline = pipeline

	if err := p.ensureSetup(); err != nil {
		return fmt.Errorf("postgres setup failed: %w", err)
	}

	return p.connectAndStartReplication(ackCh)
}

// Stop gracefully stops the replication stream and closes the connection.
func (p *PostgresSource) Stop() error {
	slog.Info("stopping postgres source")
	close(p.stop)
	if p.conn != nil {
		return p.conn.Close(context.Background())
	}
	return nil
}

// ensureSetup validates wal_level and auto-creates publication if missing.
func (p *PostgresSource) ensureSetup() error {
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		p.cfg.User, p.cfg.Password, p.cfg.Host, p.cfg.Port, p.cfg.Database)

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return fmt.Errorf("setup connection failed: %w", err)
	}
	defer conn.Close(ctx)

	if err := p.checkWalLevel(ctx, conn); err != nil {
		return err
	}
	return p.ensurePublication(ctx, conn)
}

// checkWalLevel verifies wal_level = logical.
func (p *PostgresSource) checkWalLevel(ctx context.Context, conn *pgx.Conn) error {
	var walLevel string
	if err := conn.QueryRow(ctx, "SHOW wal_level").Scan(&walLevel); err != nil {
		return fmt.Errorf("failed to check wal_level: %w", err)
	}
	if walLevel != walLevelLogical {
		return fmt.Errorf("wal_level is '%s', must be '%s' — change postgresql.conf and restart", walLevel, walLevelLogical)
	}
	slog.Info("wal_level check passed", "wal_level", walLevel)
	return nil
}

// ensurePublication creates the publication if it doesn't exist.
func (p *PostgresSource) ensurePublication(ctx context.Context, conn *pgx.Conn) error {
	pubName := p.cfg.PublicationName

	var exists bool
	if err := conn.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)", pubName).Scan(&exists); err != nil {
		return fmt.Errorf("failed to check publication: %w", err)
	}

	if exists {
		slog.Info("publication already exists", "publication", pubName)
		return nil
	}

	sql := p.buildCreatePublicationSQL(pubName)
	if _, err := conn.Exec(ctx, sql); err != nil {
		return fmt.Errorf("failed to create publication: %w", err)
	}
	slog.Info("publication created", "publication", pubName, "sql", sql)
	return nil
}

// buildCreatePublicationSQL builds the CREATE PUBLICATION statement.
func (p *PostgresSource) buildCreatePublicationSQL(pubName string) string {
	if len(p.cfg.Tables) == 0 {
		return fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES", pubName)
	}
	return fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pubName, strings.Join(p.cfg.Tables, ", "))
}

// connectAndStartReplication opens a replication connection and launches the read loop.
func (p *PostgresSource) connectAndStartReplication(ackCh <-chan uint64) error {
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?replication=database",
		p.cfg.User, p.cfg.Password, p.cfg.Host, p.cfg.Port, p.cfg.Database)

	ctx := context.Background()
	conn, err := pgconn.Connect(ctx, connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to postgres: %w", err)
	}
	p.conn = conn

	slotName := p.cfg.SlotName
	pubName := p.cfg.PublicationName

	// Create replication slot (ignore "already exists" error)
	if _, err := pglogrepl.CreateReplicationSlot(ctx, conn, slotName, outputPlugin, pglogrepl.CreateReplicationSlotOptions{
		SnapshotAction: snapshotAction,
	}); err != nil {
		slog.Warn("create replication slot (may already exist)", "slot", slotName, "err", err)
	}

	sysident, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		return fmt.Errorf("IdentifySystem failed: %w", err)
	}

	pluginArgs := []string{
		fmt.Sprintf("proto_version '%s'", protoVersion),
		fmt.Sprintf("publication_names '%s'", pubName),
	}

	if err := pglogrepl.StartReplication(ctx, conn, slotName, sysident.XLogPos, pglogrepl.StartReplicationOptions{
		PluginArgs: pluginArgs,
	}); err != nil {
		return fmt.Errorf("StartReplication failed: %w", err)
	}

	slog.Info("postgres logical replication started", "slot", slotName, "publication", pubName, "lsn", sysident.XLogPos)
	go p.ackLoop(ackCh)
	go p.readLoop(sysident.XLogPos)
	return nil
}

// ackLoop continuously receives ACKs from the pipeline and updates the highest flushed LSN.
func (p *PostgresSource) ackLoop(ackCh <-chan uint64) {
	for {
		select {
		case <-p.stop:
			return
		case lsn, ok := <-ackCh:
			if !ok {
				return
			}
			// Only update if the new LSN is greater than the current one (safety check)
			current := atomic.LoadUint64(&p.flushedLSN)
			if lsn > current {
				atomic.StoreUint64(&p.flushedLSN, lsn)
			}
		}
	}
}

// readLoop continuously reads WAL messages and dispatches CDC events.
func (p *PostgresSource) readLoop(startLSN pglogrepl.LSN) {
	clientLSN := startLSN
	nextStandby := time.Now().Add(standbyInterval)
	relations := make(map[uint32]*pglogrepl.RelationMessage)

	for {
		select {
		case <-p.stop:
			return
		default:
		}

		if time.Now().After(nextStandby) {
			p.sendStandbyUpdate(clientLSN)
			nextStandby = time.Now().Add(standbyInterval)
		}

		rawMsg, err := p.receiveMessage(nextStandby)
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			slog.Error("receive message failed", "err", err)
			return
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			slog.Error("postgres error response", "severity", errMsg.Severity, "message", errMsg.Message)
			return
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			continue
		}

		clientLSN, nextStandby = p.handleCopyData(msg, clientLSN, nextStandby, relations)
	}
}

// sendStandbyUpdate sends a WAL position update to PostgreSQL.
func (p *PostgresSource) sendStandbyUpdate(clientLSN pglogrepl.LSN) {
	flushed := pglogrepl.LSN(atomic.LoadUint64(&p.flushedLSN))

	// If no events have been flushed yet, use the initial client LSN
	flushPos := flushed
	if flushPos == 0 {
		flushPos = clientLSN
	}

	if err := pglogrepl.SendStandbyStatusUpdate(context.Background(), p.conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: clientLSN,
		WALFlushPosition: flushPos,
		WALApplyPosition: flushPos,
	}); err != nil {
		slog.Error("standby status update failed", "err", err)
	}
}

// receiveMessage reads a single message with a deadline.
func (p *PostgresSource) receiveMessage(deadline time.Time) (pgproto3.BackendMessage, error) {
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	return p.conn.ReceiveMessage(ctx)
}

// handleCopyData processes a single CopyData message (keepalive or xlog data).
func (p *PostgresSource) handleCopyData(
	msg *pgproto3.CopyData,
	clientLSN pglogrepl.LSN,
	nextStandby time.Time,
	relations map[uint32]*pglogrepl.RelationMessage,
) (pglogrepl.LSN, time.Time) {
	switch msg.Data[0] {
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		return p.handleKeepalive(msg.Data[1:], clientLSN, nextStandby)
	case pglogrepl.XLogDataByteID:
		return p.handleXLogData(msg.Data[1:], clientLSN, nextStandby, relations)
	}
	return clientLSN, nextStandby
}

// handleKeepalive processes a primary keepalive message.
func (p *PostgresSource) handleKeepalive(data []byte, clientLSN pglogrepl.LSN, nextStandby time.Time) (pglogrepl.LSN, time.Time) {
	pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(data)
	if err != nil {
		slog.Error("parse keepalive failed", "err", err)
		return clientLSN, nextStandby
	}
	if pkm.ServerWALEnd > clientLSN {
		clientLSN = pkm.ServerWALEnd
	}
	if pkm.ReplyRequested {
		nextStandby = time.Time{} // force immediate reply
	}
	return clientLSN, nextStandby
}

// handleXLogData parses xlog data and dispatches CDC events.
func (p *PostgresSource) handleXLogData(
	data []byte,
	clientLSN pglogrepl.LSN,
	nextStandby time.Time,
	relations map[uint32]*pglogrepl.RelationMessage,
) (pglogrepl.LSN, time.Time) {
	xld, err := pglogrepl.ParseXLogData(data)
	if err != nil {
		slog.Error("parse xlog data failed", "err", err)
		return clientLSN, nextStandby
	}

	logicalMsg, err := pglogrepl.Parse(xld.WALData)
	if err != nil {
		slog.Error("parse logical message failed", "err", err)
		return clientLSN, nextStandby
	}

	p.dispatchLogicalMessage(logicalMsg, relations, uint64(xld.WALStart+pglogrepl.LSN(len(xld.WALData))))
	return xld.WALStart + pglogrepl.LSN(len(xld.WALData)), nextStandby
}

// dispatchLogicalMessage routes a logical message to the appropriate handler.
func (p *PostgresSource) dispatchLogicalMessage(msg pglogrepl.Message, relations map[uint32]*pglogrepl.RelationMessage, lsn uint64) {
	switch v := msg.(type) {
	case *pglogrepl.RelationMessage:
		relations[v.RelationID] = v
	case *pglogrepl.InsertMessage:
		if rel, ok := relations[v.RelationID]; ok && p.isTableAllowed(rel.Namespace, rel.RelationName) {
			p.emitEvent(constant.CreateAction.String(), rel, nil, v.Tuple.Columns, lsn)
		}
	case *pglogrepl.UpdateMessage:
		if rel, ok := relations[v.RelationID]; ok && p.isTableAllowed(rel.Namespace, rel.RelationName) {
			var oldCols []*pglogrepl.TupleDataColumn
			if v.OldTuple != nil {
				oldCols = v.OldTuple.Columns
			}
			p.emitEvent(constant.UpdateAction.String(), rel, oldCols, v.NewTuple.Columns, lsn)
		}
	case *pglogrepl.DeleteMessage:
		if rel, ok := relations[v.RelationID]; ok && p.isTableAllowed(rel.Namespace, rel.RelationName) {
			var oldCols []*pglogrepl.TupleDataColumn
			if v.OldTuple != nil {
				oldCols = v.OldTuple.Columns
			}
			p.emitEvent(constant.DeleteAction.String(), rel, oldCols, nil, lsn)
		}
	}
}

// isTableAllowed checks the table filter. Empty list means all tables allowed.
func (p *PostgresSource) isTableAllowed(namespace, table string) bool {
	if len(p.tableMap) == 0 {
		return true
	}
	return p.tableMap[table] || p.tableMap[fmt.Sprintf("%s.%s", namespace, table)]
}

// emitEvent converts WAL tuple data into a models.Event and sends it to the pipeline.
func (p *PostgresSource) emitEvent(op string, rel *pglogrepl.RelationMessage, oldCols, newCols []*pglogrepl.TupleDataColumn, lsn uint64) {
	before := decodeTupleAsJSON(rel, oldCols)
	after := decodeTupleAsJSON(rel, newCols)
	slog.Debug("cdc event", "op", op, "schema", rel.Namespace, "table", rel.RelationName)
	p.pipeline <- models.NewEvent(op, rel.Namespace, rel.RelationName, before, after, lsn)
}

// escapeJSON safely escapes double quotes and backslashes in JSON strings.
func escapeJSON(s string) string {
	b, _ := json.Marshal(s)
	// Marshal includes leading and trailing quotes, we strip them.
	return string(b[1 : len(b)-1])
}

// decodeTupleAsJSON maps WAL column data directly to a JSON byte array.
func decodeTupleAsJSON(rel *pglogrepl.RelationMessage, cols []*pglogrepl.TupleDataColumn) json.RawMessage {
	if cols == nil {
		return nil
	}

	var buf strings.Builder
	buf.WriteByte('{')

	first := true
	for i, col := range cols {
		if i >= len(rel.Columns) {
			break
		}
		
		if col.DataType == 'u' {
			continue // 'u' = unchanged TOAST
		}

		if !first {
			buf.WriteByte(',')
		}
		first = false

		name := rel.Columns[i].Name
		oid := rel.Columns[i].DataType

		buf.WriteString(`"` + escapeJSON(name) + `":`)

		switch col.DataType {
		case 't': // text representation
			writeColumnJSON(&buf, string(col.Data), oid)
		case 'n': // explicit NULL
			buf.WriteString("null")
		}
	}
	buf.WriteByte('}')
	return json.RawMessage(buf.String())
}

// PostgreSQL OIDs — https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat
const (
	oidBool        = 16
	oidInt2        = 21
	oidInt4        = 23
	oidInt8        = 20
	oidFloat4      = 700
	oidFloat8      = 701
	oidNumeric     = 1700
	oidTimestamp   = 1114
	oidTimestampTZ = 1184
	oidDate        = 1082
	oidJSON        = 114
	oidJSONB       = 3802
	oidUUID        = 2950
	oidText        = 25
	oidVarchar     = 1043
	oidBpchar      = 1042 // char(n)
)

// writeStringJSON writes a JSON encoded string to the buffer
func writeStringJSON(buf *strings.Builder, s string) {
	b, _ := json.Marshal(s)
	buf.Write(b)
}

// writeColumnJSON converts a text-encoded PG value to JSON bytes.
func writeColumnJSON(buf *strings.Builder, raw string, oid uint32) {
	switch oid {
	case oidBool:
		if raw == "t" || raw == "true" {
			buf.WriteString("true")
		} else {
			buf.WriteString("false")
		}
	case oidInt2, oidInt4, oidInt8:
		if _, err := strconv.ParseInt(raw, 10, 64); err == nil {
			buf.WriteString(raw)
		} else {
			writeStringJSON(buf, raw)
		}
	case oidFloat4, oidFloat8, oidNumeric:
		if _, err := strconv.ParseFloat(raw, 64); err == nil {
			buf.WriteString(raw)
		} else {
			writeStringJSON(buf, raw)
		}

	case oidTimestamp:
		// PG format: "2026-03-10 09:19:25.788595"
		for _, layout := range pgTimestampLayouts {
			if t, err := time.Parse(layout, raw); err == nil {
				buf.WriteString(strconv.FormatInt(t.UnixMilli(), 10))
				return
			}
		}
		writeStringJSON(buf, raw)

	case oidTimestampTZ:
		// PG format: "2026-03-10 09:19:25.788595+00"
		for _, layout := range pgTimestampTZLayouts {
			if t, err := time.Parse(layout, raw); err == nil {
				buf.WriteString(strconv.FormatInt(t.UnixMilli(), 10))
				return
			}
		}
		writeStringJSON(buf, raw)

	case oidDate:
		if t, err := time.Parse(time.DateOnly, raw); err == nil {
			buf.WriteString(strconv.FormatInt(t.UnixMilli(), 10))
			return
		}
		writeStringJSON(buf, raw)

	case oidJSON, oidJSONB:
		buf.WriteString(raw)

	default:
		// text, varchar, uuid, and everything else → keep as string
		writeStringJSON(buf, raw)
	}
}

// Timestamp layouts for parsing PG timestamp without timezone.
var pgTimestampLayouts = []string{
	"2006-01-02 15:04:05.999999",
	"2006-01-02 15:04:05",
}

// Timestamp layouts for parsing PG timestamptz.
var pgTimestampTZLayouts = []string{
	"2006-01-02 15:04:05.999999-07",
	"2006-01-02 15:04:05.999999-07:00",
	"2006-01-02 15:04:05-07",
	"2006-01-02 15:04:05-07:00",
}
