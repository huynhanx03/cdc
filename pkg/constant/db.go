package constant

type ActionDB string

const (
	CreateAction ActionDB = "c"
	UpdateAction ActionDB = "u"
	DeleteAction ActionDB = "d"
)

func (a ActionDB) String() string {
	return string(a)
}
