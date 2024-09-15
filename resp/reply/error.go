package reply

type UnknownErrReply struct {
}

var unknownErrBytes = []byte("-Err unknown\r\n")

func (u UnknownErrReply) Error() string {
	return "Err unknown"
}

func (u UnknownErrReply) ToBytes() []byte {
	return unknownErrBytes
}

type ArgNumErrReply struct {
	Cmd string
}

func (a *ArgNumErrReply) Error() string {
	return "-Err wrong number of arguments for '" + a.Cmd + "' command\r\n"
}

func (a *ArgNumErrReply) ToBytes() []byte {
	//TODO implement me
	return []byte("-Err wrong number of arguments for '" + a.Cmd + "' command\r\n")
}

func MakeArgNumErrReply(cmd string) *ArgNumErrReply {
	return &ArgNumErrReply{
		Cmd: cmd,
	}
}

type SyntaxErrReply struct{}

var syntaxErrBytes = []byte("-Err syntax error\r\n")
var theSyntaxErrReply = &SyntaxErrReply{}

func (r *SyntaxErrReply) Error() string {
	return "Err syntax error"
}
func (r *SyntaxErrReply) ToBytes() []byte {
	return syntaxErrBytes
}

func MakeSyntaxErrReply() *SyntaxErrReply {
	return theSyntaxErrReply
}

type WrongTypeErrReply struct{}

var wrongTypeErrBytes = []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")

func (r *WrongTypeErrReply) ToBytes() []byte {
	return wrongTypeErrBytes
}

func (r *WrongTypeErrReply) Error() string {
	return "WRONGTYPE Operation against a key holding the wrong kind of value"
}

type ProtocolErrReply struct {
	Msg string
}

func (r *ProtocolErrReply) ToBytes() []byte {
	return []byte("-ERR Protocol error: '" + r.Msg + "'\r\n")
}

func (r *ProtocolErrReply) Error() string {
	return "ERR Protocol error: '" + r.Msg
}
