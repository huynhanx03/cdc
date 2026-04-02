package constant

type SourceType string

const (
	SourceTypePostgres SourceType = "postgres"
	SourceTypeMySQL    SourceType = "mysql"
	SourceTypeMariaDB  SourceType = "mariadb"
	SourceTypeREST     SourceType = "rest"
)

func (s SourceType) String() string {
	return string(s)
}
