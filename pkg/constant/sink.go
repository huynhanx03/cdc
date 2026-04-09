package constant

type SinkType string

const (
	SinkTypeElasticsearch SinkType = "elasticsearch"
	SinkTypeStdout        SinkType = "stdout"
	SinkTypePostgres      SinkType = "postgres"
	SinkTypeWebhook       SinkType = "webhook"
	SinkTypeRedis         SinkType = "redis"
	SinkTypeClickhouse    SinkType = "clickhouse"
)

func (s SinkType) String() string {
	return string(s)
}
