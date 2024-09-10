package bloom

import (
	"reflect"
	"unsafe"
)

func unsafeStringToBytes(s string) (b []byte) {
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	bh.Data = sh.Data
	bh.Cap = sh.Len
	bh.Len = sh.Len
	return b
}

func unsafeBytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
