package constant

type ActionDB string

const (
	CreateAction   ActionDB = "c"
	UpdateAction   ActionDB = "u"
	DeleteAction   ActionDB = "d"
	SnapshotAction ActionDB = "s"
)

func (a ActionDB) String() string {
	return string(a)
}
