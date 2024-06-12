package partition

import (
	"strings"
	"testing"
)

func TestGetToken(t *testing.T) {
	if n := getToken("nimbus"); n != 164 {
		t.Error("error", n)
	}

	var rbs strings.Builder
	rbs.Grow(1 << 30)
	for i := 0; i < 1<<30; i++ {
		rbs.WriteByte(0)
	}
	if n := getToken(rbs.String()); n != 84 {
		t.Error("error", n)
	}
}
