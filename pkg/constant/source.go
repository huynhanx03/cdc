package constant

type SourceType string

const (
	SourceTypePostgres SourceType = "postgres"
)

func (s SourceType) String() string {
	return string(s)
}
