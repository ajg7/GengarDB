package storage

import (
	"errors"
	"path/filepath"
	"testing"
)

func openHF(t *testing.T) *HeapFile {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "heap.bin")
	hf, err := OpenHeapFile(path)
	if err != nil {
		t.Fatalf("open heap: %v", err)
	}
	return hf
}

func TestHeap_InsertGet_Delete(t *testing.T) {
	hf := openHF(t)
	defer hf.Close()

	records := [][]byte{
		[]byte("alpha"),
		[]byte("bravo-bravo"),
		[]byte("charlie the third"),
	}

	var rids []RID
	for _, r := range records {
		rid, err := hf.Insert(r)
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		rids = append(rids, rid)
	}

	// Read back
	for i, rid := range rids {
		got, err := hf.Get(rid)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if string(got) != string(records[i]) {
			t.Fatalf("mismatch: want %q got %q", records[i], got)
		}
	}

	// Delete middle and confirm not found
	if err := hf.Delete(rids[1]); err != nil {
		t.Fatalf("delete: %v", err)
	}
	_, err := hf.Get(rids[1])
	if !errors.Is(err, ErrSlotDeleted) {
		t.Fatalf("expected ErrSlotDeleted, got %v", err)
	}
}

func TestHeap_MultipageGrowth(t *testing.T) {
	hf := openHF(t)
	defer hf.Close()

	// Large-ish records to force multiple pages.
	payload := make([]byte, 900) // 900 * 6 ~= 5400 > 1 page
	var rids []RID
	for i := 0; i < 6; i++ {
		for j := range payload {
			payload[j] = byte((i + j) % 251)
		}
		rid, err := hf.Insert(payload)
		if err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
		rids = append(rids, rid)
	}

	// Verify all readable via Scan and direct Get.
	count := 0
	err := hf.Scan(func(r RID, data []byte) bool {
		count++
		if len(data) != 900 {
			t.Fatalf("scan length: got %d", len(data))
		}
		return true
	})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if count != len(rids) {
		t.Fatalf("scan count mismatch: %d vs %d", count, len(rids))
	}

	for _, rid := range rids {
		got, err := hf.Get(rid)
		if err != nil || len(got) != 900 {
			t.Fatalf("get rid %+v: err=%v len=%d", rid, err, len(got))
		}
	}
}
