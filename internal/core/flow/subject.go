package flow

import (
	"encoding/base64"
	"strings"
)

func CDCSubject(sourceID, schema, table, partition string) string {
	return strings.Join([]string{
		"cdc",
		encodeSubjectToken(sourceID),
		encodeSubjectToken(schema),
		encodeSubjectToken(table),
		encodeSubjectToken(partition),
	}, ".")
}

func CDCFilterSubject(sourceID, schema, table string) string {
	return strings.Join([]string{
		"cdc",
		encodeSubjectToken(sourceID),
		encodeSubjectToken(schema),
		encodeSubjectToken(table),
		"*",
	}, ".")
}

func encodeSubjectToken(token string) string {
	if token == "" {
		return "_"
	}
	return base64.RawURLEncoding.EncodeToString([]byte(token))
}
