package storage

import "testing"

func TestSetAndGet(t *testing.T) {
	s := NewStorage()
	_, err := s.Get("hasan")
	if err == nil {
		t.Error("expected error when retrieving non-existing value")
	}

	s.Set("hasan", "hooshang")
	val, err := s.Get("hasan")
	if val != "hooshang" {
		t.Error("expected to fetch value for the provided key")
	}
	if err != nil {
		t.Error(err)
	}
}
