package encoder

type OpType uint8

const (
	OpTypeDelete OpType = iota
	OpTypeSet
)

type Encoder struct{}

func NewEncoder() *Encoder {
	return &Encoder{}
}

func (e *Encoder) Encode(op OpType, val []byte) []byte {
	n := len(val)
	buf := make([]byte, n+1)
	buf[0] = byte(op)
	copy(buf[1:], val)
	return buf
}

func (e *Encoder) Decode(buf []byte) *EncodedValue {
	return &EncodedValue{
		val:    buf[1:],
		OpType: OpType(buf[0]),
	}
}

type EncodedValue struct {
	val    []byte
	OpType OpType
}

func (ev *EncodedValue) Value() []byte {
	return ev.val
}

func (ev *EncodedValue) IsTombstone() bool {
	return ev.OpType == OpTypeDelete
}
