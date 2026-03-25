package mysql

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/foden/cdc/pkg/config"
	"github.com/foden/cdc/pkg/constant"
	"github.com/foden/cdc/pkg/interfaces"
	"github.com/foden/cdc/pkg/models"
	"github.com/foden/cdc/pkg/registry"
)

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
	}, nil
}

// Start initializes the canal and begins streaming events.
func (s *MySQLSource) Start(pipeline chan<- *models.Event, ackCh <-chan uint64, initialOffset string) error {
	s.pipeline = pipeline

	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port)
	cfg.User = s.cfg.Username
	cfg.Password = s.cfg.Password
	cfg.Charset = "utf8mb4"
	cfg.Flavor = "mysql" // Or "mariadb" if needed

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

	// Run canal. This is blocking.
	go func() {
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
			if !strings.Contains(err.Error(), "closed") {
				slog.Error("mysql canal run failed", "err", err)
			}
		}
	}()

	return nil
}

func (s *MySQLSource) Stop() error {
	slog.Info("stopping mysql source", "instance", s.cfg.InstanceID)
	close(s.stop)
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
	action := constant.CreateAction.String()
	switch e.Action {
	case canal.UpdateAction:
		action = constant.UpdateAction.String()
	case canal.DeleteAction:
		action = constant.DeleteAction.String()
	}

	for i := 0; i < len(e.Rows); {
		var before, after json.RawMessage

		switch e.Action {
		case canal.InsertAction:
			after = h.rowToJSON(e, e.Rows[i])
			i++
		case canal.DeleteAction:
			before = h.rowToJSON(e, e.Rows[i])
			i++
		case canal.UpdateAction:
			// Update comes in pairs: Old, New
			if i+1 < len(e.Rows) {
				before = h.rowToJSON(e, e.Rows[i])
				after = h.rowToJSON(e, e.Rows[i+1])
			}
			i += 2
		}

		// Use log position as LSN for now (uint64)
		lsn := uint64(e.Header.LogPos)

		// Get current synced position for the generic offset string
		pos := h.source.canal.SyncedPosition()
		offset := fmt.Sprintf("%s:%d", pos.Name, pos.Pos)

		h.source.pipeline <- models.NewEvent(
			action,
			h.source.cfg.InstanceID,
			e.Table.Schema,
			e.Table.Name,
			before,
			after,
			lsn,
			offset,
		)
	}

	return nil
}

func (h *eventHandler) rowToJSON(e *canal.RowsEvent, row []interface{}) json.RawMessage {
	res := make(map[string]interface{}, len(row))
	for i, val := range row {
		name := e.Table.Columns[i].Name
		res[name] = formatValue(val)
	}
	b, _ := json.Marshal(res)
	return json.RawMessage(b)
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
