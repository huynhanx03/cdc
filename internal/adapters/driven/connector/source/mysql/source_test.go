package mysql

import (
	"strings"
	"testing"

	"github.com/foden/cdc/internal/core/ports"
	coreruntime "github.com/foden/cdc/internal/core/runtime"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

func TestParseMySQLPositionOffset(t *testing.T) {
	pos, ok := parseMySQLPositionOffset("mysql-bin.000123:456")
	if !ok {
		t.Fatal("parseMySQLPositionOffset returned ok=false")
	}
	if pos.Name != "mysql-bin.000123" || pos.Pos != 456 {
		t.Fatalf("position = %+v, want mysql-bin.000123:456", pos)
	}
}

func TestParseMySQLPositionOffsetRejectsInvalid(t *testing.T) {
	tests := []string{
		"",
		"mysql-bin.000123",
		"mysql-bin.000123:not-a-number",
		"mysql-bin.000123:4294967296",
		":123",
	}

	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			if pos, ok := parseMySQLPositionOffset(input); ok {
				t.Fatalf("parseMySQLPositionOffset(%q) = %+v, true; want false", input, pos)
			}
		})
	}
}

func TestParseDDLAffectedTable(t *testing.T) {
	tests := []struct {
		query      string
		wantOp     string
		wantSchema string
		wantTable  string
		wantOK     bool
	}{
		{query: "ALTER TABLE users ADD COLUMN email text", wantOp: "alter", wantSchema: "app", wantTable: "users", wantOK: true},
		{query: "DROP TABLE IF EXISTS app.users", wantOp: "drop", wantSchema: "app", wantTable: "users", wantOK: true},
		{query: "RENAME TABLE `app`.`users` TO `app`.`users_old`", wantOp: "rename", wantSchema: "app", wantTable: "users", wantOK: true},
		{query: "CREATE INDEX idx ON users(id)", wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			op, schemaName, tableName, ok := parseDDLAffectedTable("app", tt.query)
			if ok != tt.wantOK || op != tt.wantOp || schemaName != tt.wantSchema || tableName != tt.wantTable {
				t.Fatalf("parseDDLAffectedTable = %q,%q,%q,%v; want %q,%q,%q,%v", op, schemaName, tableName, ok, tt.wantOp, tt.wantSchema, tt.wantTable, tt.wantOK)
			}
		})
	}
}

func TestOnDDLRejectsDropForSelectedTable(t *testing.T) {
	reg := coreruntime.NewRegistry()
	if err := reg.RegisterFlow(&ports.FlowConfig{
		FlowID:      "flow-1",
		SourceID:    "src-1",
		SinkID:      "sink-1",
		SourceTable: "app.users",
		SinkTable:   "app.users",
		Options:     &ports.FlowOptions{PartitionCount: 4},
	}); err != nil {
		t.Fatal(err)
	}
	source := &MySQLSource{
		cfg:             &ports.SourceConfig{InstanceID: "src-1"},
		runtimeRegistry: reg,
	}
	handler := &eventHandler{source: source}

	err := handler.OnDDL(nil, mysql.Position{}, &replication.QueryEvent{
		Schema: []byte("app"),
		Query:  []byte("DROP TABLE users"),
	})
	if err == nil || !strings.Contains(err.Error(), "affects selected table") {
		t.Fatalf("err = %v, want selected-table DDL error", err)
	}
}
