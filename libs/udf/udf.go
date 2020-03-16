package main

import (
	"C"
	_ "runtime/cgo"
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

//export ArrayAvg
func ArrayAvg(numbers *C.double, size C.int) C.double {
	return C.double(float64(ArraySum(numbers, size)) / float64(size))
}

func main() {}
