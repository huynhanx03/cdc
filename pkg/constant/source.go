package constant

type SourceType string

const (
	SourceTypePostgres SourceType = "postgres"
	SourceTypeMySQL    SourceType = "mysql"
	SourceTypeMariaDB  SourceType = "mariadb"
)

func (s SourceType) String() string {
	return string(s)
}
