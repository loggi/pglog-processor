package main

import (
	"testing"
//	"time"
	"fmt"
	"encoding/json"
)

const JSON_DATA = `
{
  "overall_checkpoint": {},
  "normalyzed_info": {
    "commit;": {
      "chronos": {
        "20151006": {
          "18": {
            "count": 22,
            "duration": 7369.941,
            "min": {"00":3,"01":3,"02":4,"05":1,"07":1,"09":3,"10":7},
            "min_duration": {"00": 233.06,"01": 215.289,"02": 253.358,"05": 58.471,"07": 131.922,"09": 278.288,"10": 896.483}
          },
          "19": {
            "count": 6,
            "duration": 7369.941,
            "min": {"00":3,"01":3},
            "min_duration": {"00": 233.06,"01": 215.289}
          }
        }
      }
    }
  },
  "top_slowest": [
    ["151.536","2015-09-25 16:53:55","SELECT 1","user1","db1",null,null,null,null],
    ["147.257","2015-09-25 16:53:16","SELECT 2","user2","db2",null,null,null,null]
  ]
}`

func TestConversionTopSlowest(t *testing.T) {
	res := convert([]byte(JSON_DATA))
	fmt.Println(len(res))
}

func TestUnmarshalMarshal(t *testing.T) {
	o := PgBadgerOutputData{}
	json.Unmarshal([]byte(JSON_DATA), &o)
	fmt.Println(o)
	if _, err := json.Marshal(o); err != nil {
		panic(o)
	}
}

func BenchmarkConversion(b *testing.B) {
	for i := 0; i < b.N; i++ {
		convert([]byte(JSON_DATA))
	}
}
