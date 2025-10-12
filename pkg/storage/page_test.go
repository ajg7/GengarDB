package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func openTempFile(t *testing.T, name string) *os.File {
	t.Helper()
	dir := t.TempDir()
	fp := filepath.Join(dir, name)
	f, err := os.OpenFile(fp, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o666)
	if err != nil {
		t.Fatalf("open temp file: %v", err)
	}
	return f
}

func TestPage_RoundTrip(t *testing.T) {
	f := openTempFile(t, "pages.bin")
	defer f.Close()

	// write two pages
	payloads := []string{"hello gengar", "page two test data"}
	for i, s := range payloads {
		var p Page
		p.ID = uint32(i)
		if err := p.SetData([]byte(s)); err != nil {
			t.Fatalf("SetData: %v", err)
		}
		if err := WritePage(f, &p); err != nil {
			t.Fatalf("WritePage: %v", err)
		}
	}

	// read them back
	for i, want := range payloads {
		gotp, err := ReadPage(f, uint32(i))
		if err != nil {
			t.Fatalf("ReadPage: %v", err)
		}
		got := string(gotp.Data[:gotp.DataSize])
		if got != want {
			t.Fatalf("payload mismatch: want %q, got %q", want, got)
		}
	}
}

func TestPage_ChecksumDetectsCorruption(t *testing.T) {
	f := openTempFile(t, "corrupt.bin")
	defer f.Close()

	var p Page
	p.ID = 7
	_ = p.SetData([]byte("integrity!"))
	if err := WritePage(f, &p); err != nil {
		t.Fatalf("WritePage: %v", err)
	}

	// Flip a byte in the payload region on disk.
	pos := pageOffset(p.ID) + HeaderSize // first payload byte
	orig := []byte{0}
	if _, err := f.ReadAt(orig, pos); err == nil {
		orig[0] ^= 0xFF
		if _, err := f.WriteAt(orig, pos); err != nil {
			t.Fatalf("corrupt write: %v", err)
		}
	} else {
		// If read fails (unlikely), just force a value.
		if _, err := f.WriteAt([]byte{0xFF}, pos); err != nil {
			t.Fatalf("corrupt write fallback: %v", err)
		}
	}
	_ = f.Sync()

	_, err := ReadPage(f, p.ID)
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("expected ErrChecksumMismatch, got %v", err)
	}
}

func TestPage_DataTooLarge(t *testing.T) {
	var p Page
	tooBig := make([]byte, PayloadSize+1)
	if err := p.SetData(tooBig); !errors.Is(err, ErrDataTooLarge) {
		t.Fatalf("expected ErrDataTooLarge, got %v", err)
	}
}
