package main

import (
	"testing"
//	"time"
	"fmt"
)

const JSON_DATA = `{"overall_checkpoint": {}, "top_slowest": [["151.536","2015-09-25 16:53:55","SELECT 1","srv1","app1",null,null,null,null],["147.257","2015-09-25 16:53:16","SELECT 2","srv2","app2",null,null,null,null]]}`

func TestConversion(t *testing.T) {
	res := convert([]byte(JSON_DATA))

	fmt.Println(len(res))
}

func BenchmarkConversion(b *testing.B) {
	for i := 0; i < b.N; i++ {
		convert([]byte(JSON_DATA))
	}
}

