package index

import (
	"path/filepath"
	"testing"

	"gengardb/pkg/storage"
)

func openTree(t *testing.T) *BTree {
	t.Helper()
	dir := t.TempDir()
	fp := filepath.Join(dir, "idx.bin")
	tr, err := Open(fp)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return tr
}

func TestBTree_InsertAndGet_Sequential(t *testing.T) {
	tr := openTree(t)
	defer tr.Close()

	const N = 2000 // big enough to force multiple splits
	for i := uint64(1); i <= N; i++ {
		r := storage.RID{PageID: uint32(i % 1234), SlotID: uint16(i % 4096)}
		if err := tr.Insert(i, r); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	for i := uint64(1); i <= N; i++ {
		r, ok, err := tr.Get(i)
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if !ok {
			t.Fatalf("missing key %d", i)
		}
		if uint32(i%1234) != r.PageID || uint16(i%4096) != r.SlotID {
			t.Fatalf("rid mismatch for %d: got %+v", i, r)
		}
	}
}

func TestBTree_DuplicateKeyRejected(t *testing.T) {
	tr := openTree(t)
	defer tr.Close()

	r := storage.RID{PageID: 1, SlotID: 1}
	if err := tr.Insert(42, r); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := tr.Insert(42, r); err == nil {
		t.Fatalf("expected duplicate key error")
	}
}

func TestBTree_SpansMultipleLevels(t *testing.T) {
	tr := openTree(t)
	defer tr.Close()

	// Insert blocks of keys to ensure multiple internal levels.
	const N = 10000
	for i := uint64(10); i < 10+N; i += 10 {
		if err := tr.Insert(i, storage.RID{PageID: uint32(i), SlotID: uint16(i)}); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	// Probe random-ish subset
	for i := uint64(10); i < 10+N; i += 123 {
		r, ok, err := tr.Get(i)
		if err != nil || !ok || r.PageID != uint32(i) {
			t.Fatalf("lookup %d failed: ok=%v err=%v rid=%+v", i, ok, err, r)
		}
	}
}
