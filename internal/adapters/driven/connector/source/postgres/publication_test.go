package postgres

import (
	"testing"

	"github.com/foden/cdc/internal/core/ports"
)

func TestCreatePublicationSQLQuotesTables(t *testing.T) {
	sql := createPublicationSQL("cdc_pub_src", []ports.SourceTableRef{{Schema: "public", Table: "users"}})
	want := `CREATE PUBLICATION "cdc_pub_src" FOR TABLE "public"."users"`
	if sql != want {
		t.Fatalf("sql = %q, want %q", sql, want)
	}
}

func TestDedupeTablesDefaultsSchema(t *testing.T) {
	got := dedupeTables([]ports.SourceTableRef{{Table: "users"}, {Schema: "public", Table: "users"}, {Table: ""}})
	if len(got) != 1 || got[0].Schema != "public" || got[0].Table != "users" {
		t.Fatalf("tables = %+v", got)
	}
}

func TestCreatePublicationSQLSortsAndQuotesTables(t *testing.T) {
	sql := createPublicationSQL("cdc_pub_src", []ports.SourceTableRef{
		{Schema: "sales", Table: "orders"},
		{Schema: "public", Table: `user"audit`},
		{Schema: "sales", Table: "orders"},
	})
	want := `CREATE PUBLICATION "cdc_pub_src" FOR TABLE "public"."user""audit", "sales"."orders"`
	if sql != want {
		t.Fatalf("sql = %q, want %q", sql, want)
	}
}
