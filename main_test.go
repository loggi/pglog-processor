package pglog_processor

import (
	"testing"
//	"time"
	"fmt"
	"encoding/json"
	"strings"
)

const JSON_DATA = `
{
  "overall_checkpoint": {},
  "normalyzed_info": {
    "select 1;": {
      "chronos": {
        "20151006": {
          "18": {
            "count": 7,
            "duration": 7369.941,
            "min": {"00":3,"01":3,"02":4,"05":1,"07":1,"09":3,"10":7},
            "min_duration": {"00": 233.06,"01": 215.289,"02": 253.358,"05": 58.471,"07": 131.922,"09": 278.288,"10": 896.483}
          },
          "19": {
            "count": 2,
            "duration": 7369.941,
            "min": {"00":3,"01":3},
            "min_duration": {"00": 233.06,"01": 215.289}
          }
        }
      }
    },
    "commit;": {
      "chronos": {
        "20151006": {
          "18": {
            "count": 7,
            "duration": 7369.941,
            "min": {"00":3},
            "min_duration": {"00": 233.06}
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

const EMPTY_DATA = ``

func TestConversion(t *testing.T) {
	res, err := convert([]byte(JSON_DATA))
	if err != nil {
		t.Errorf("Error converting")
	}
	fmt.Println(len(res))

	sres := string(res)
	if !strings.Contains(sres,nfoActionKeyOnES) {
		t.Errorf("Should have generated %v json data", nfoActionKeyOnES)
	}
	if !strings.Contains(sres,tslActionKeyOnES) {
		t.Errorf("Should have generated %v json data", tslActionKeyOnES)
	}
	for _, blacklisted := range config.Main.BlacklistedQuery {
		if strings.Contains(sres, blacklisted) {
			t.Errorf("Shouldn't have generated data containg %v", blacklisted)
		}
	}

	fmt.Println(sres)
}

func TestEmptyConversionError(t *testing.T) {
	res, err := convert([]byte(EMPTY_DATA))
	if err == nil {
		t.Errorf("Should have created Error")
	}
	fmt.Println(len(res))
}

func TestUnmarshal(t *testing.T) {
	o := PgBadgerOutputData{}
	json.Unmarshal([]byte(JSON_DATA), &o)

	if len(o.PgBadgerTopSlowest) != 2 {
		t.Errorf("Should have unmarshalled 2 top slowest elements, instead got %v when unmarshalled `%v`",
			len(o.PgBadgerTopSlowest),
			o.PgBadgerTopSlowest)
	}
	if len(o.PgBadgerNormalyzedInfo.Entries) != 10 {
		t.Errorf("Should have unmarshalled 10 normalized info elements, instead got %v when unmarshalled `%v`",
			len(o.PgBadgerNormalyzedInfo.Entries),
			o.PgBadgerNormalyzedInfo)
	}
}

func BenchmarkConversion(b *testing.B) {
	for i := 0; i < b.N; i++ {
		convert([]byte(JSON_DATA))
	}
}
