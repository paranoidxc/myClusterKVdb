package resp

type Connection interface {
	Write(data []byte) error
	GetDBIndex() int
	SelectDB(index int)
}
