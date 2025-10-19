package storage

import (
	"errors"
	"os"
)

// HeapFile stores slotted pages back-to-back inside a single disk file.
// The heap grows by appending new pages whenever existing ones run out of room.
type HeapFile struct {
	f *os.File
}

// OpenHeapFile creates or opens the heap file on disk so pages can be read/written.
func OpenHeapFile(path string) (*HeapFile, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o666)
	if err != nil {
		return nil, err
	}
	return &HeapFile{f: f}, nil
}

func (hf *HeapFile) Close() error { return hf.f.Close() }

func (hf *HeapFile) pageCount() (uint32, error) {
	st, err := hf.f.Stat()
	if err != nil {
		return 0, err
	}
	// Page count is derived from file size; pages are fixed width so byte math is simple.
	return uint32(st.Size() / PageSize), nil
}

func (hf *HeapFile) findPageWithSpace(need int) (uint32, *SlottedPage, *Page, error) {
	n, err := hf.pageCount()
	if err != nil {
		return 0, nil, nil, err
	}
	for id := uint32(0); id < n; id++ {
		p, err := ReadPage(hf.f, id)
		if err != nil {
			return 0, nil, nil, err
		}
		sp := NewSlottedPage(p)
		sp.InitIfFresh()
		if sp.freeSpace() >= need {
			return id, sp, p, nil
		}
	}
	// No page had room; allocate a brand new empty page in memory.
	newID := n
	p := &Page{ID: newID}
	sp := NewSlottedPage(p)
	sp.InitIfFresh()
	return newID, sp, p, nil
}

// Insert places rec into the heap and returns its RID.
func (hf *HeapFile) Insert(rec []byte) (RID, error) {
	need := len(rec) + slotEntrySize
	id, sp, p, err := hf.findPageWithSpace(need)
	if err != nil {
		return RID{}, err
	}
	slot, err := sp.Insert(rec)
	if err != nil {
		return RID{}, err
	}
	if err := WritePage(hf.f, p); err != nil {
		return RID{}, err
	}
	return RID{PageID: id, SlotID: slot}, nil
}

// Get reads a record by RID.
func (hf *HeapFile) Get(r RID) ([]byte, error) {
	p, err := ReadPage(hf.f, r.PageID)
	if err != nil {
		return nil, err
	}
	sp := NewSlottedPage(p)
	return sp.Read(r.SlotID)
}

// Delete marks the record as deleted.
func (hf *HeapFile) Delete(r RID) error {
	p, err := ReadPage(hf.f, r.PageID)
	if err != nil {
		return err
	}
	sp := NewSlottedPage(p)
	if err := sp.Delete(r.SlotID); err != nil {
		return err
	}
	return WritePage(hf.f, p)
}

// Optional convenience: full scan (used in tests).
func (hf *HeapFile) Scan(visit func(r RID, data []byte) bool) error {
	n, err := hf.pageCount()
	if err != nil {
		return err
	}
	for id := uint32(0); id < n; id++ {
		p, err := ReadPage(hf.f, id)
		if err != nil {
			return err
		}
		sp := NewSlottedPage(p)
		sc, _, _ := sp.header()
		// Iterate slot directory, skipping slots that have been lazily deleted.
		for s := uint16(0); s < sc; s++ {
			b, err := sp.Read(s)
			if err != nil {
				if errors.Is(err, ErrSlotDeleted) {
					continue
				}
				return err
			}
			if !visit(RID{PageID: id, SlotID: s}, b) {
				return nil
			}
		}
	}
	return nil
}
