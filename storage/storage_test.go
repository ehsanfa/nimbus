package storage

import "testing"

func TestSetAndGet(t *testing.T) {
	s := newStorage()
	_, err := s.get("hasan")
	if err == nil {
		t.Error("expected error when retrieving non-existing value")
	}

	s.set("hasan", "hooshang")
	val, err := s.get("hasan")
	if val != "hooshang" {
		t.Error("expected to fetch value for the provided key")
	}
	if err != nil {
		t.Error(err)
	}
}
