package mysql

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"

	"github.com/foden/cdc/pkg/config"
	"github.com/foden/cdc/pkg/constant"
	"github.com/foden/cdc/pkg/interfaces"
	"github.com/foden/cdc/pkg/models"
	"github.com/foden/cdc/pkg/registry"
)

const (
	_defaultResetBackoff = 1 * time.Second
	_maxResetBackoff     = 30 * time.Second
)

// mysqlTask represents a row change from binlog
type mysqlTask struct {
	op     string
	db     string
	table  *schema.Table
	before []interface{}
	after  []interface{}
	lsn    uint64
	offset string
	ts     int64
}

// Pool for reusing maps to reduce GC pressure
var columnPool = sync.Pool{
	New: func() interface{} {
		return make(map[string]interface{}, 32)
	},
}

func init() {
	registry.RegisterSource(constant.SourceTypeMySQL.String(), NewMySQLSource)
	registry.RegisterSource(constant.SourceTypeMariaDB.String(), NewMySQLSource)
}

func NewMySQLSource(cfg *config.SourceConfig) (interfaces.Source, error) {
	return New(cfg)
}

// MySQLSource streams CDC events via MySQL binlog replication.
type MySQLSource struct {
	cfg      *config.SourceConfig
	canal    *canal.Canal
	pipeline chan<- *models.Event
	tableMap map[string]bool
	stop     chan struct{}

	// Single worker for strict ordering
	taskChan chan *mysqlTask
	wg       sync.WaitGroup
}

// New creates a MySQLSource.
func New(cfg *config.SourceConfig) (*MySQLSource, error) {
	tm := make(map[string]bool, len(cfg.Tables))
	for _, t := range cfg.Tables {
		tm[t] = true
	}
	return &MySQLSource{
		cfg:      cfg,
		tableMap: tm,
		stop:     make(chan struct{}),
		taskChan: make(chan *mysqlTask, 8192),
	}, nil
}

// Start initializes the canal and begins streaming events.
func (s *MySQLSource) Start(pipeline chan<- *models.Event, ackCh <-chan uint64, initialOffset string) error {
	s.pipeline = pipeline

	// Start exactly ONE worker to guarantee 100% order
	s.wg.Add(1)
	go s.singleOrderedWorker()

	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port)
	cfg.User = s.cfg.Username
	cfg.Password = s.cfg.Password
	cfg.Charset = "utf8mb4"
	if s.cfg.Type == constant.SourceTypeMariaDB.String() {
		cfg.Flavor = "mariadb"
	} else {
		cfg.Flavor = "mysql"
	}

	// Only include specified tables if the list is not empty
	if len(s.cfg.Tables) > 0 {
		cfg.IncludeTableRegex = make([]string, 0, len(s.cfg.Tables))
		for _, t := range s.cfg.Tables {
			// Convert "db.table" to regex db\.table
			regex := strings.ReplaceAll(t, ".", "\\.")
			cfg.IncludeTableRegex = append(cfg.IncludeTableRegex, "^"+regex+"$")
		}
	}

	c, err := canal.NewCanal(cfg)
	if err != nil {
		return fmt.Errorf("failed to create canal: %w", err)
	}
	s.canal = c

	// Set event handler
	s.canal.SetEventHandler(&eventHandler{source: s})

	slog.Info("mysql replication starting", "host", s.cfg.Host, "instance", s.cfg.InstanceID, "offset", initialOffset)

	// Start ack loop
	go s.ackLoop(ackCh)

	// Run canal with auto-reconnect
	go func() {
		backoff := _defaultResetBackoff

		for {
			select {
			case <-s.stop:
				return
			default:
			}

			var err error
			if initialOffset != "" {
				parts := strings.Split(initialOffset, ":")
				if len(parts) == 2 {
					pos, pErr := strconv.ParseUint(parts[1], 10, 32)
					if pErr == nil {
						mysqlPos := mysql.Position{Name: parts[0], Pos: uint32(pos)}
						err = s.canal.RunFrom(mysqlPos)
					} else {
						err = s.canal.Run()
					}
				} else {
					err = s.canal.Run()
				}
			} else {
				err = s.canal.Run()
			}

			if err != nil {
				if strings.Contains(err.Error(), "closed") {
					return // Graceful shutdown
				}

				slog.Error("mysql canal run failed, reconnecting",
					"err", err,
					"instance", s.cfg.InstanceID,
					"backoff", backoff)

				select {
				case <-time.After(backoff):
				case <-s.stop:
					return
				}

				backoff *= 2
				if backoff > _maxResetBackoff {
					backoff = _maxResetBackoff
				}

				// Update offset from last synced position for resume
				pos := s.canal.SyncedPosition()
				initialOffset = fmt.Sprintf("%s:%d", pos.Name, pos.Pos)
				continue
			}
			return
		}
	}()

	return nil
}

func (s *MySQLSource) Stop() error {
	slog.Info("stopping mysql source", "instance", s.cfg.InstanceID)
	close(s.stop)
	close(s.taskChan) // Signal worker to stop
	s.wg.Wait()       // Wait for processing to finish

	if s.canal != nil {
		s.canal.Close()
	}
	return nil
}

func (s *MySQLSource) InstanceID() string {
	return s.cfg.InstanceID
}

func (s *MySQLSource) ackLoop(ackCh <-chan uint64) {
	for {
		select {
		case <-s.stop:
			return
		case _, ok := <-ackCh:
			if !ok {
				return
			}
			// In MySQL, binlog positions are handled differently,
			// typically using gtid or file+pos. For this MVP,
			// we don't persist positions yet.
		}
	}
}

type eventHandler struct {
	canal.DummyEventHandler
	source *MySQLSource
}

func (h *eventHandler) OnRow(e *canal.RowsEvent) error {
	for i := 0; i < len(e.Rows); {
		var before, after []interface{}
		op := "u"

		switch e.Action {
		case canal.InsertAction:
			op = "c"
			after = e.Rows[i]
			i++
		case canal.DeleteAction:
			op = "d"
			before = e.Rows[i]
			i++
		case canal.UpdateAction:
			op = "u"
			if i+1 < len(e.Rows) {
				before = e.Rows[i]
				after = e.Rows[i+1]
			}
			i += 2
		}

		lsn := uint64(e.Header.LogPos)
		pos := h.source.canal.SyncedPosition()
		offset := fmt.Sprintf("%s:%d", pos.Name, pos.Pos)

		h.source.taskChan <- &mysqlTask{
			op:     op,
			db:     e.Table.Schema,
			table:  e.Table,
			before: before,
			after:  after,
			lsn:    lsn,
			offset: offset,
			ts:     time.Now().UnixMilli(),
		}
	}
	return nil
}

func (s *MySQLSource) singleOrderedWorker() {
	defer s.wg.Done()
	for task := range s.taskChan {
		s.processTask(task)
	}
}

func (s *MySQLSource) processTask(t *mysqlTask) {
	before := s.rowToMap(t.table, t.before)
	after := s.rowToMap(t.table, t.after)

	beforeData, _ := sonic.Marshal(before)
	afterData, _ := sonic.Marshal(after)

	// Recycle maps back to pool
	if before != nil {
		s.releaseMap(before)
	}
	if after != nil {
		s.releaseMap(after)
	}

	payload := models.DebeziumPayload{
		Op:     t.op,
		Before: json.RawMessage(beforeData),
		After:  json.RawMessage(afterData),
		Source: models.SourceMetadata{
			Version:   "1.0",
			Connector: "mysql",
			Name:      s.cfg.InstanceID,
			TsMs:      t.ts,
			Snapshot:  "false",
			DB:        t.db,
			Schema:    t.db,
			Table:     t.table.Name,
			LSN:       t.lsn,
		},
		TimestampMS: time.Now().UnixMilli(),
	}

	data, _ := sonic.Marshal(payload)

	topic := s.cfg.Topic
	if topic == "" {
		topic = "cdc"
	}

	subject := fmt.Sprintf("%s.%s.%s.%s", topic, s.cfg.InstanceID, t.db, t.table.Name)
	s.pipeline <- models.NewEvent(
		topic,
		subject,
		s.cfg.InstanceID,
		t.db,
		t.table.Name,
		t.op,
		t.lsn,
		t.offset,
		data,
	)
}

func (s *MySQLSource) rowToMap(table *schema.Table, row []interface{}) map[string]interface{} {
	if row == nil {
		return nil
	}

	obj := columnPool.Get().(map[string]interface{})
	for i, val := range row {
		name := table.Columns[i].Name
		obj[name] = formatValue(val)
	}
	return obj
}

func (s *MySQLSource) releaseMap(m map[string]interface{}) {
	for k := range m {
		delete(m, k)
	}
	columnPool.Put(m)
}

func formatValue(val interface{}) interface{} {
	switch v := val.(type) {
	case []byte:
		return string(v)
	default:
		return v
	}
}

func (h *eventHandler) OnRotate(header *replication.EventHeader, rotateEvent *replication.RotateEvent) error {
	return nil
}

func (h *eventHandler) OnDDL(header *replication.EventHeader, nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	return nil
}

func (h *eventHandler) String() string {
	return "MySQLCDCEventHandler"
}
