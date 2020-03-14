package main

import (
	"C"
	"unsafe"
)

//export ArraySum
func ArraySum(numbers *C.double, size C.int) C.double {
	p := uintptr(unsafe.Pointer(numbers))
    arrLen := int(size)

    var sum float64
    sum = 0

	for i := 0; i < arrLen; i++ {
		number := *(*float64)(unsafe.Pointer(p))
		sum += number
		p += unsafe.Sizeof(number)
	}

	return C.double(sum)
}

func main() {}
