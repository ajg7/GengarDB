package index

// B-Tree implementation tuned for fixed-size pages stored on disk.
// Nodes are read and written through storage.Page, so we keep the
// in-memory view extremely small and encode data into raw bytes.
import (
	"encoding/binary"
	"errors"
	"os"
	"sort"

	"gengardb/pkg/storage"
)

const (
	// kind* constants label what type of node a page represents.
	// We only track metadata, internal index nodes, and leaf nodes that hold data.
	kindMeta     = 0
	kindInternal = 1
	kindLeaf     = 2

	// Layout sizes (in bytes) for encoded pages. Keeping these together makes the
	// on-disk format easier to reason about while reading the code.
	nodeHdrSize      = 16
	leafEntrySize    = 16 // key(8) + page(4) + slot(2) + pad(2)
	internalFirstKid = 4
	internalEntSize  = 12 // key(8) + rightChild(4)
)

var (
	ErrNotFound   = errors.New("btree: key not found")
	ErrDupKey     = errors.New("btree: duplicate key")
	ErrCorruption = errors.New("btree: corrupt node")
)

// BTree wraps a set of on-disk pages backed by storage.Page records.
// All operations start from rootID and pull nodes from the file handle.
type BTree struct {
	f      *os.File
	rootID uint32
}

// ----- open/close/meta -----

// Open sets up a B-Tree file. If the file is empty, we bootstrap meta/root pages.
func Open(path string) (*BTree, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o666)
	if err != nil {
		return nil, err
	}
	t := &BTree{f: f}

	// Empty file => bootstrap meta + root leaf so we have a usable tree from day one.
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if st.Size() == 0 {
		// page 0: meta
		meta := &storage.Page{ID: 0}
		meta.DataSize = storage.PayloadSize
		setNodeHeader(meta.Data[:], kindMeta, 0, 0xFFFFFFFF, 0)
		if err := storage.WritePage(f, meta); err != nil {
			_ = f.Close()
			return nil, err
		}
		// page 1: root leaf
		root := &storage.Page{ID: 1}
		root.DataSize = storage.PayloadSize
		setNodeHeader(root.Data[:], kindLeaf, 0, 0xFFFFFFFF, 0)
		if err := storage.WritePage(f, root); err != nil {
			_ = f.Close()
			return nil, err
		}
		// Record root in meta.aux so future Opens can resume from this root page.
		setMetaRoot(meta.Data[:], 1)
		if err := storage.WritePage(f, meta); err != nil {
			_ = f.Close()
			return nil, err
		}
		t.rootID = 1
		return t, nil
	}

	// Existing tree: read meta page 0 to find the saved root page.
	meta, err := storage.ReadPage(f, 0)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if nodeKind(meta.Data[:]) != kindMeta {
		_ = f.Close()
		return nil, ErrCorruption
	}
	t.rootID = metaRoot(meta.Data[:])
	return t, nil
}

func (t *BTree) Close() error { return t.f.Close() }

// ----- public API -----

// Insert adds a key->RID mapping to the tree. We enforce unique keys to keep the
// example simple. Splits bubble up until the tree is balanced again.
func (t *BTree) Insert(key uint64, rid storage.RID) error {
	leaf, err := t.findLeaf(t.rootID, key)
	if err != nil {
		return err
	}
	// decode leaf
	lp := leaf
	if nodeKind(lp.Data[:]) != kindLeaf {
		return ErrCorruption
	}
	keys, vals := leafLeafEntries(lp)
	// sort.Search keeps tree operations logarithmic by binary searching the slice.
	i := sort.Search(len(keys), func(i int) bool { return key <= keys[i] })
	if i < len(keys) && keys[i] == key {
		return ErrDupKey
	}

	// insert in-memory slices
	keys = insertU64(keys, i, key)
	vals = insertRID(vals, i, rid)

	// If the leaf still fits within the page budget, write the updated node and we are done.
	if len(keys) <= leafCapacity() {
		writeLeaf(lp, keys, vals)
		return storage.WritePage(t.f, lp)
	}

	// Otherwise split the leaf, write both halves, and promote the separator key.
	rightKeys, rightVals := splitLeafArrays(&keys, &vals)
	// left written back
	writeLeaf(lp, keys, vals)
	if err := storage.WritePage(t.f, lp); err != nil {
		return err
	}

	// new right node
	rightID, rp, err := t.allocPage(kindLeaf)
	if err != nil {
		return err
	}
	writeLeaf(rp, rightKeys, rightVals)
	// parent pointers remain implicit; we don't store them (kept in header but not used in this minimal version)
	if err := storage.WritePage(t.f, rp); err != nil {
		return err
	}

	// promote first key of right node into parent
	sep := rightKeys[0]
	return t.insertIntoParent(leaf.ID, sep, rightID)
}

// Get performs the standard B-Tree point lookup and returns (rid, true) when found.
func (t *BTree) Get(key uint64) (storage.RID, bool, error) {
	leaf, err := t.findLeaf(t.rootID, key)
	if err != nil {
		return storage.RID{}, false, err
	}
	keys, vals := leafLeafEntries(leaf)
	i := sort.Search(len(keys), func(i int) bool { return key <= keys[i] })
	if i < len(keys) && i >= 0 && len(keys) > 0 && keys[i] == key {
		return vals[i], true, nil
	}
	return storage.RID{}, false, nil
}

// ----- insert helpers -----

func (t *BTree) insertIntoParent(leftID uint32, key uint64, rightID uint32) error {
	// If left is root, we grew the tree height. Create a fresh root node.
	if leftID == t.rootID {
		rootID, p, err := t.allocPage(kindInternal)
		if err != nil {
			return err
		}
		writeInternalRoot(p, leftID, []uint64{key}, []uint32{rightID})
		if err := storage.WritePage(t.f, p); err != nil {
			return err
		}
		// update meta root
		meta, err := storage.ReadPage(t.f, 0)
		if err != nil {
			return err
		}
		setMetaRoot(meta.Data[:], rootID)
		if err := storage.WritePage(t.f, meta); err != nil {
			return err
		}
		t.rootID = rootID
		return nil
	}

	// Otherwise, find parent by descending from root (no explicit parent pointers stored).
	parent, idx, err := t.findParentAndIndex(t.rootID, leftID, key)
	if err != nil {
		return err
	}
	// decode parent
	pkeys, kids := internalEntries(parent)
	// parent children layout: firstChild, then (key,rightKid)...
	// We know left child is at position idx in kids (the left of (key,right)).
	// Insert (key,rightID) after that position.
	pkeys = insertU64(pkeys, idx, key)
	kids = insertU32(kids, idx+1, rightID)

	if len(pkeys) <= internalCapacity() {
		writeInternal(parent, pkeys, kids)
		return storage.WritePage(t.f, parent)
	}

	// Parent overflow triggers another split and the separator keeps propagating upward.
	rightKeys, rightKids := splitInternalArrays(&pkeys, &kids)
	writeInternal(parent, pkeys, kids)
	if err := storage.WritePage(t.f, parent); err != nil {
		return err
	}
	rid, rp, err := t.allocPage(kindInternal)
	if err != nil {
		return err
	}
	writeInternal(rp, rightKeys, rightKids)
	if err := storage.WritePage(t.f, rp); err != nil {
		return err
	}
	// Promote middle key (first key of right half is the separator).
	sep := rightKeys[0]
	return t.insertIntoParent(parent.ID, sep, rid)
}

// findLeaf walks down from nodeID to the correct leaf by following search keys.
func (t *BTree) findLeaf(nodeID uint32, key uint64) (*storage.Page, error) {
	id := nodeID
	for {
		p, err := storage.ReadPage(t.f, id)
		if err != nil {
			return nil, err
		}
		switch nodeKind(p.Data[:]) {
		case kindLeaf:
			return p, nil
		case kindInternal:
			keys, kids := internalEntries(p)
			// choose child i where key < keys[i]; kids is always one element longer than keys.
			i := sort.Search(len(keys), func(i int) bool { return key < keys[i] })
			id = kids[i]
		default:
			return nil, ErrCorruption
		}
	}
}

// findParentAndIndex locates the parent whose child pointer matches childID.
// We redo the descent from the root each time to stay stateless inside nodes.
func (t *BTree) findParentAndIndex(currID, childID uint32, key uint64) (*storage.Page, int, error) {
	// descend until we reach a node whose one of the children == childID
	p, err := storage.ReadPage(t.f, currID)
	if err != nil {
		return nil, 0, err
	}
	if nodeKind(p.Data[:]) == kindLeaf {
		return nil, 0, ErrCorruption
	}
	keys, kids := internalEntries(p)
	for i := 0; i < len(kids); i++ {
		if kids[i] == childID {
			return p, i, nil
		}
	}
	// choose child to continue (like search)
	i := sort.Search(len(keys), func(i int) bool { return key < keys[i] })
	return t.findParentAndIndex(kids[i], childID, key)
}

// ----- encoding/decoding -----
// The helpers below pack and unpack Go slices into the raw byte layout used by storage.Page.

func nodeKind(d []byte) byte { return d[0] }

func setNodeHeader(d []byte, kind byte, count uint16, parent uint32, aux uint32) {
	d[0] = kind
	d[1] = 0
	binary.LittleEndian.PutUint16(d[2:4], count)
	binary.LittleEndian.PutUint32(d[4:8], parent)
	binary.LittleEndian.PutUint32(d[8:12], aux)
	// d[12:16] reserved
}

// metaRoot reads the root pointer stored in the metadata page.
func metaRoot(d []byte) uint32 { return binary.LittleEndian.Uint32(d[8:12]) }

// setMetaRoot persists a new root pointer into the metadata page.
func setMetaRoot(d []byte, root uint32) {
	binary.LittleEndian.PutUint32(d[8:12], root)
}

func leafCapacity() int {
	return (storage.PayloadSize - nodeHdrSize) / leafEntrySize
}
func internalCapacity() int {
	return (storage.PayloadSize - nodeHdrSize - internalFirstKid) / internalEntSize
}

// leafLeafEntries decodes the key/value pairs from a leaf page into Go slices.
func leafLeafEntries(p *storage.Page) ([]uint64, []storage.RID) {
	cnt := int(binary.LittleEndian.Uint16(p.Data[2:4]))
	keys := make([]uint64, cnt)
	vals := make([]storage.RID, cnt)
	off := nodeHdrSize
	for i := 0; i < cnt; i++ {
		keys[i] = binary.LittleEndian.Uint64(p.Data[off : off+8])
		page := binary.LittleEndian.Uint32(p.Data[off+8 : off+12])
		slot := binary.LittleEndian.Uint16(p.Data[off+12 : off+14])
		vals[i] = storage.RID{PageID: page, SlotID: slot}
		off += leafEntrySize
	}
	return keys, vals
}

// writeLeaf encodes the provided keys/RIDs back into the on-page format.
func writeLeaf(p *storage.Page, keys []uint64, vals []storage.RID) {
	setNodeHeader(p.Data[:], kindLeaf, uint16(len(keys)), 0xFFFFFFFF, 0)
	off := nodeHdrSize
	for i := 0; i < len(keys); i++ {
		binary.LittleEndian.PutUint64(p.Data[off:off+8], keys[i])
		binary.LittleEndian.PutUint32(p.Data[off+8:off+12], vals[i].PageID)
		binary.LittleEndian.PutUint16(p.Data[off+12:off+14], vals[i].SlotID)
		// off+14..16 padding
		off += leafEntrySize
	}
	// clear remainder (optional)
	for j := off; j < storage.PayloadSize; j++ {
		p.Data[j] = 0
	}
	p.DataSize = storage.PayloadSize
}

// internalEntries decodes an internal node into a key slice and a child pointer slice.
func internalEntries(p *storage.Page) ([]uint64, []uint32) {
	cnt := int(binary.LittleEndian.Uint16(p.Data[2:4]))
	keys := make([]uint64, cnt)
	kids := make([]uint32, cnt+1)
	off := nodeHdrSize
	kids[0] = binary.LittleEndian.Uint32(p.Data[off : off+4])
	off += internalFirstKid
	for i := 0; i < cnt; i++ {
		keys[i] = binary.LittleEndian.Uint64(p.Data[off : off+8])
		kids[i+1] = binary.LittleEndian.Uint32(p.Data[off+8 : off+12])
		off += internalEntSize
	}
	return keys, kids
}

// writeInternal encodes an internal node which always has len(keys)+1 child pointers.
func writeInternal(p *storage.Page, keys []uint64, kids []uint32) {
	setNodeHeader(p.Data[:], kindInternal, uint16(len(keys)), 0xFFFFFFFF, 0)
	off := nodeHdrSize
	binary.LittleEndian.PutUint32(p.Data[off:off+4], kids[0])
	off += internalFirstKid
	for i := 0; i < len(keys); i++ {
		binary.LittleEndian.PutUint64(p.Data[off:off+8], keys[i])
		binary.LittleEndian.PutUint32(p.Data[off+8:off+12], kids[i+1])
		off += internalEntSize
	}
	for j := off; j < storage.PayloadSize; j++ {
		p.Data[j] = 0
	}
	p.DataSize = storage.PayloadSize
}

// writeInternalRoot is a thin wrapper used when promoting a new root node.
func writeInternalRoot(p *storage.Page, left uint32, keys []uint64, rightKids []uint32) {
	// rightKids must have len == len(keys)
	kids := make([]uint32, len(keys)+1)
	kids[0] = left
	copy(kids[1:], rightKids)
	writeInternal(p, keys, kids)
}

// ----- array ops & splits -----

// insertU* helpers shift slices to make room for a new element.
func insertU64(a []uint64, i int, v uint64) []uint64 {
	a = append(a, 0)
	copy(a[i+1:], a[i:])
	a[i] = v
	return a
}
func insertU32(a []uint32, i int, v uint32) []uint32 {
	a = append(a, 0)
	copy(a[i+1:], a[i:])
	a[i] = v
	return a
}
func insertRID(a []storage.RID, i int, v storage.RID) []storage.RID {
	a = append(a, storage.RID{})
	copy(a[i+1:], a[i:])
	a[i] = v
	return a
}

// splitLeafArrays halves a leaf node and returns the right-hand slice copies.
func splitLeafArrays(keys *[]uint64, vals *[]storage.RID) ([]uint64, []storage.RID) {
	k := *keys
	v := *vals
	mid := len(k) / 2
	rightK := append([]uint64(nil), k[mid:]...)
	rightV := append([]storage.RID(nil), v[mid:]...)
	*keys = k[:mid]
	*vals = v[:mid]
	return rightK, rightV
}

func splitInternalArrays(keys *[]uint64, kids *[]uint32) ([]uint64, []uint32) {
	k := *keys
	c := *kids
	mid := len(k) / 2
	// Right side keeps keys[mid:] and children[mid+1:]
	rightK := append([]uint64(nil), k[mid:]...)
	rightC := append([]uint32(nil), c[mid+1:]...)
	*keys = k[:mid]
	*kids = c[:mid+1]
	return rightK, rightC
}

// ----- allocation -----

// allocPage appends a fresh, zeroed page to the file and returns it for writing.
func (t *BTree) allocPage(kind byte) (uint32, *storage.Page, error) {
	st, err := t.f.Stat()
	if err != nil {
		return 0, nil, err
	}
	next := uint32(st.Size() / storage.PageSize)
	p := &storage.Page{ID: next}
	p.DataSize = storage.PayloadSize
	setNodeHeader(p.Data[:], kind, 0, 0xFFFFFFFF, 0)
	return next, p, nil
}
