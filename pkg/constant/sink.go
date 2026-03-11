package constant

type SinkType string

const (
	SinkTypeElasticsearch SinkType = "elasticsearch"
	SinkTypeStdout        SinkType = "stdout"
)

func (s SinkType) String() string {
	return string(s)
}
