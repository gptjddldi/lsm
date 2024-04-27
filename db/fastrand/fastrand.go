package fastrand

import _ "unsafe"

//go:linkname Uint32 runtime.fastrand
func Uint32() uint32
