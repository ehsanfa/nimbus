package partition

import (
	"fmt"
	"strings"
	"testing"
)

func TestGetToken(t *testing.T) {
	if n := GetToken("nimbus", 180); n != 164 {
		t.Error("error", n)
	}

	var rbs strings.Builder
	rbs.Grow(1 << 30)
	for i := 0; i < 1<<30; i++ {
		rbs.WriteByte(0)
	}
	if n := GetToken(rbs.String(), 180); n != 84 {
		t.Error("error", n)
	}
}

func TestSuggestPartition(t *testing.T) {
	var maxParts int64 = 360
	cases := map[int64]int64{
		0: 0,
		1: 180,
		2: 90,
		3: 270,
	}
	for numberOfNodes, expected := range cases {
		if p := SuggestPartition(maxParts, numberOfNodes); p != expected {
			t.Error(fmt.Sprintf("for %d number of nodes expected %d, received %d", numberOfNodes, expected, p))
		}
	}

}
