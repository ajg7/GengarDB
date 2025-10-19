package storage

import (
	"encoding/binary"
	"errors"
)

// Slotted pages divide the on-disk page payload into three regions:
//   * A small fixed-size header at the start that tracks free space and slot count.
//   * User payload data that grows forward from the header as records are added.
//   * A slot directory at the end that grows backward and stores (offset,length) pairs
//     pointing into the payload region. Each record has a slot entry, enabling updates
//     without shuffling existing payload bytes.
const (
	spHeaderSize   = 6  // slotCount(2) + freeStart(2) + freeEnd(2)
	slotEntrySize  = 4  // offset(2) + length(2)
)

var (
	ErrNoSpace       = errors.New("storage: not enough free space on page")
	ErrSlotDeleted   = errors.New("storage: slot deleted")
	ErrBadSlotID     = errors.New("storage: invalid slot id")
)

// RID identifies a record within the heap file.
type RID struct {
	PageID uint32
	SlotID uint16
}

// SlottedPage is a view over Page.Data implementing a slotted layout.
type SlottedPage struct{ p *Page }

func NewSlottedPage(p *Page) *SlottedPage {
	return &SlottedPage{p: p}
}

// Initialize the slotted header if this is a fresh page.
func (sp *SlottedPage) InitIfFresh() {
	sc, fs, fe := sp.header()
	if sc == 0 && fs == 0 && fe == 0 {
		// Newly zeroed pages report empty metadata, so seed the header with
		// an empty slot directory and the entire payload marked free.
		sp.setHeader(0, spHeaderSize, PayloadSize)
	}
	if sp.p.DataSize == 0 {
		// Cover the whole payload by checksum for slotted pages.
		sp.p.DataSize = PayloadSize
	}
}

func (sp *SlottedPage) header() (slotCount, freeStart, freeEnd uint16) {
	d := sp.p.Data[:]
	slotCount = binary.LittleEndian.Uint16(d[0:2])
	freeStart = binary.LittleEndian.Uint16(d[2:4])
	freeEnd = binary.LittleEndian.Uint16(d[4:6])
	return
}

func (sp *SlottedPage) setHeader(slotCount, freeStart, freeEnd uint16) {
	d := sp.p.Data[:]
	binary.LittleEndian.PutUint16(d[0:2], slotCount)
	binary.LittleEndian.PutUint16(d[2:4], freeStart)
	binary.LittleEndian.PutUint16(d[4:6], freeEnd)
	// Keep checksum region covering the entire payload area.
	sp.p.DataSize = PayloadSize
}

func (sp *SlottedPage) freeSpace() int {
	sc, fs, fe := sp.header()
	// Free bytes equal the hole between payload growth (freeStart) and slot
	// directory growth (freeEnd), minus space reserved for new slot entries.
	return int(fe) - int(fs) - int(sc)*slotEntrySize
}

func slotPos(index uint16) int {
	// Slots live at the end of the page in reverse index order.
	return PayloadSize - int(index+1)*slotEntrySize
}

func (sp *SlottedPage) getSlot(i uint16) (off, ln uint16, err error) {
	sc, _, _ := sp.header()
	if i >= sc {
		return 0, 0, ErrBadSlotID
	}
	pos := slotPos(i)
	d := sp.p.Data[:]
	off = binary.LittleEndian.Uint16(d[pos : pos+2])
	ln = binary.LittleEndian.Uint16(d[pos+2 : pos+4])
	return
}

func (sp *SlottedPage) setSlot(i, off, ln uint16) {
	pos := slotPos(i)
	d := sp.p.Data[:]
	binary.LittleEndian.PutUint16(d[pos:pos+2], off)
	binary.LittleEndian.PutUint16(d[pos+2:pos+4], ln)
}

// Insert appends a new record; returns SlotID.
func (sp *SlottedPage) Insert(rec []byte) (uint16, error) {
	if len(rec) > 0xFFFF {
		// keep encoding simple (uint16 length)
		return 0, ErrDataTooLarge
	}
	req := len(rec) + slotEntrySize
	if sp.freeSpace() < req {
		return 0, ErrNoSpace
	}

	sc, fs, fe := sp.header()
	// Write record bytes into the payload region at freeStart.
	copy(sp.p.Data[fs:], rec)
	// Reserve slot
	slotID := sc
	sp.setSlot(slotID, fs, uint16(len(rec)))
	// Update header
	sc++
	fs += uint16(len(rec))
	fe -= slotEntrySize
	sp.setHeader(sc, fs, fe)
	return slotID, nil
}

// Read returns a copy of the record bytes for slot i.
func (sp *SlottedPage) Read(i uint16) ([]byte, error) {
	off, ln, err := sp.getSlot(i)
	if err != nil {
		return nil, err
	}
	if ln == 0 {
		return nil, ErrSlotDeleted
	}
	// Return a defensive copy so callers cannot mutate the page buffer.
	out := make([]byte, ln)
	copy(out, sp.p.Data[off:int(off)+int(ln)])
	return out, nil
}

// Delete marks the slot as deleted (lazy delete).
func (sp *SlottedPage) Delete(i uint16) error {
	off, _, err := sp.getSlot(i)
	if err != nil {
		return err
	}
	// Clear length but keep offset so we can reclaim space later if desired.
	sp.setSlot(i, off, 0)
	return nil
}
