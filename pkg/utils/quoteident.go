package utils

import (
	"strings"
)

// QuoteIdent quotes a SQL identifier with double-quotes.
func QuoteIdent(name string) string {
	if strings.Contains(name, ".") {
		parts := strings.SplitN(name, ".", 2)
		return quoteIdentPart(parts[0]) + "." + quoteIdentPart(parts[1])
	}
	return quoteIdentPart(name)
}

// QuoteIdentMySQL quotes an identifier with backticks.
func QuoteIdentMySQL(name string) string {
	if strings.Contains(name, ".") {
		parts := strings.SplitN(name, ".", 2)
		return quoteBacktick(parts[0]) + "." + quoteBacktick(parts[1])
	}
	return quoteBacktick(name)
}

// quoteIdentPart wraps a single identifier in double-quotes.
func quoteIdentPart(s string) string {
	return "\"" + strings.ReplaceAll(s, "\"", "\"\"") + "\""
}

// quoteBacktick wraps a single identifier in backticks.
func quoteBacktick(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

// SplitTableIdent separates a full string into schema and table name.
func SplitTableIdent(s string) (string, string) {
	parts := strings.SplitN(s, ".", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	// Defaults to "public" schema if not specified
	return "public", s
}
