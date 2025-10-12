// Package storage implements the fundamental storage layer for GengarDB.
// This package handles how data is organized and stored on disk using a page-based system.
package storage

import (
	"encoding/binary" // For converting between Go data types and byte arrays
	"errors"          // For creating custom error types
	"hash/crc32"      // For computing checksums to detect data corruption
	"os"              // For file operations
)

// Constants defining the page structure for our database
const (
	// PageSize is the fixed size of each page in bytes (4KB)
	// This is a common size used by many databases as it matches typical OS page sizes
	PageSize = 4096
	
	// HeaderSize is the number of bytes reserved at the beginning of each page
	// for metadata (page ID, checksum, and data size)
	HeaderSize = 10
	
	// PayloadSize is the number of bytes available for actual data storage
	// after accounting for the header overhead
	PayloadSize = PageSize - HeaderSize
)

// Error variables define specific error conditions that can occur during page operations
var (
	// ErrChecksumMismatch indicates that the stored checksum doesn't match the computed checksum
	// This suggests data corruption has occurred
	ErrChecksumMismatch = errors.New("storage: checksum mismatch")
	
	// ErrDataTooLarge indicates that the data being stored exceeds the maximum payload size
	ErrDataTooLarge = errors.New("storage: data too large for page payload")
)

// Page represents a single page of data in our database storage system.
// Think of a page like a single "sheet" in a filing cabinet - it has a fixed size
// and contains both metadata (header) and actual data (payload).
type Page struct {
	// ID is a unique identifier for this page (like a page number)
	// uint32 allows for about 4 billion unique pages
	ID uint32
	
	// Checksum is a calculated value used to detect data corruption
	// It's computed from the actual data and stored alongside it
	Checksum uint32
	
	// DataSize tracks how many bytes of actual data are stored in this page
	// Since pages have a fixed size, not all space may be used
	DataSize uint16
	
	// Data is the actual storage area for user data
	// It's a fixed-size array that can hold up to PayloadSize bytes
	Data [PayloadSize]byte
}

// ComputeChecksum calculates a checksum for the data currently stored in the page.
// A checksum is like a "fingerprint" of the data - if the data changes, the checksum changes too.
// This helps us detect if data has been corrupted (accidentally modified).
// CRC32 is a fast and widely-used checksum algorithm.
func (p *Page) ComputeChecksum() uint32 {
	// Only compute checksum for the actual data (up to DataSize bytes)
	// The [:p.DataSize] syntax creates a slice from the beginning up to DataSize
	return crc32.ChecksumIEEE(p.Data[:p.DataSize])
}

// SetData stores the provided byte data into this page.
// It handles validation, copying the data, and cleaning up unused space.
func (p *Page) SetData(b []byte) error {
	// First, check if the data fits in our page's payload area
	if len(b) > PayloadSize {
		return ErrDataTooLarge
	}
	
	// Record how much data we're actually storing
	p.DataSize = uint16(len(b))
	
	// Copy the provided data into our page's data array
	// copy() is a built-in Go function that safely copies between slices/arrays
	copy(p.Data[:], b)
	
	// Zero out any unused bytes in the data array for consistency
	// This ensures that leftover data from previous operations doesn't interfere
	for i := int(p.DataSize); i < PayloadSize; i++ {
		p.Data[i] = 0
	}

	return nil
}

// pageOffset calculates the byte position where a specific page should be located in the file.
// Since all pages have the same size, we can calculate any page's location using simple math:
// Page 0 starts at byte 0, Page 1 starts at byte 4096, Page 2 starts at byte 8192, etc.
func pageOffset(id uint32) int64 {
	// Multiply page ID by page size to get the file offset
	// We convert to int64 to handle large file sizes (int64 can represent very large numbers)
	return int64(id) * int64(PageSize)
}

// WritePage saves a page to disk at the correct location.
// This function handles the complex process of converting our Page struct
// into the raw bytes that get stored in the file.
func WritePage(f *os.File, p *Page) error {
	// Safety check: ensure the data size is valid
	if int(p.DataSize) > PayloadSize {
		return ErrDataTooLarge
	}

	// Calculate and store the checksum before writing
	// This ensures data integrity can be verified when reading back
	p.Checksum = p.ComputeChecksum()

	// Create a buffer to hold the entire page as it will appear on disk
	buf := make([]byte, PageSize)
	
	// Serialize the page header into bytes using little-endian format
	// Little-endian is a byte ordering convention (least significant byte first)
	// [0:4] means "bytes 0 through 3" - this stores the page ID
	binary.LittleEndian.PutUint32(buf[0:4], p.ID)
	// [4:8] means "bytes 4 through 7" - this stores the checksum
	binary.LittleEndian.PutUint32(buf[4:8], p.Checksum)
	// [8:10] means "bytes 8 and 9" - this stores the data size
	binary.LittleEndian.PutUint16(buf[8:10], p.DataSize)
	
	// Copy the actual data after the header
	copy(buf[HeaderSize:], p.Data[:])

	// Write the entire page buffer to the file at the calculated offset
	// WriteAt() writes to a specific position in the file without changing the file pointer
	if _, err := f.WriteAt(buf, pageOffset(p.ID)); err != nil {
		return err
	}

	// Force the operating system to write data from memory to disk immediately
	// This ensures data is persisted even if the program crashes
	return f.Sync()
}

// ReadPage loads a page from disk and reconstructs it as a Page struct.
// This is the reverse operation of WritePage - it reads raw bytes from disk
// and converts them back into a usable Go data structure.
func ReadPage(f *os.File, id uint32) (*Page, error) {
	// Create a buffer to hold the raw page data from disk
	buf := make([]byte, PageSize)
	
	// Read the entire page from the file at the calculated offset
	// ReadAt() reads from a specific position without changing the file pointer
	if _, err := f.ReadAt(buf, pageOffset(id)); err != nil {
		return nil, err
	}

	// Parse the header bytes back into Go data types
	// This reverses the serialization process from WritePage
	p := &Page{
		// Extract the page ID from bytes 0-3
		ID: binary.LittleEndian.Uint32(buf[0:4]),
		// Extract the stored checksum from bytes 4-7
		Checksum: binary.LittleEndian.Uint32(buf[4:8]),
		// Extract the data size from bytes 8-9
		DataSize: binary.LittleEndian.Uint16(buf[8:10]),
	}
	
	// Copy the payload data (everything after the header) into the page
	copy(p.Data[:], buf[HeaderSize:])

	// Verify data integrity by comparing stored checksum with computed checksum
	// If they don't match, the data has been corrupted
	if p.ComputeChecksum() != p.Checksum {
		return nil, ErrChecksumMismatch
	}

	// Return the successfully reconstructed page
	return p, nil
}