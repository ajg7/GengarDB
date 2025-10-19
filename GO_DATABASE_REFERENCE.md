# Go Language and Database Development Reference

## Table of Contents

1. [Go Language Fundamentals](#go-language-fundamentals)
2. [Database Concepts](#database-concepts)
3. [Go Database Programming](#go-database-programming)
4. [Storage Engine Concepts](#storage-engine-concepts)
5. [Indexing and B-Trees](#indexing-and-b-trees)
6. [Transaction Management](#transaction-management)
7. [Write-Ahead Logging (WAL)](#write-ahead-logging-wal)
8. [Memory Management](#memory-management)
9. [Concurrency Patterns](#concurrency-patterns)
10. [Testing and Benchmarking](#testing-and-benchmarking)

---

## Go Language Fundamentals

### **Core Concepts**

- **`package`**: The fundamental unit of code organization in Go
- **`interface{}`**: Go's way of achieving polymorphism through duck typing
- **`struct`**: Composite data type that groups together data elements
- **`pointer`**: Variable that stores the memory address of another variable
- **`slice`**: Dynamic array that can grow and shrink at runtime
- **`map`**: Hash table implementation for key-value storage
- **`channel`**: Communication mechanism between goroutines
- **`goroutine`**: Lightweight thread managed by Go runtime

### **Memory Management**

- **Garbage Collector (GC)**: Automatic memory management system
- **Stack vs Heap**: Stack for local variables, heap for dynamic allocation
- **`make()`**: Allocates and initializes slices, maps, and channels
- **`new()`**: Allocates memory for a type and returns a pointer

### **Error Handling**

```go
// Idiomatic error handling pattern
if err != nil {
    return nil, fmt.Errorf("operation failed: %w", err)
}
```

- **`error`**: Built-in interface type for representing error conditions
- **Error Wrapping**: Using `fmt.Errorf()` with `%w` verb to wrap errors
- **Error Unwrapping**: Using `errors.Unwrap()` to access wrapped errors

---

## Database Concepts

### **ACID Properties**

- **Atomicity**: All operations in a transaction succeed or fail together
- **Consistency**: Database remains in a valid state after transactions
- **Isolation**: Concurrent transactions don't interfere with each other
- **Durability**: Committed transactions survive system failures

### **Storage Hierarchy**

- **Primary Storage**: RAM (volatile, fast access)
- **Secondary Storage**: Disk/SSD (persistent, slower access)
- **Cache**: Temporary storage for frequently accessed data
- **Buffer Pool**: In-memory cache for database pages

### **Database Architecture Components**

- **Storage Engine**: Manages how data is stored and retrieved
- **Query Processor**: Parses and executes SQL queries
- **Transaction Manager**: Ensures ACID properties
- **Lock Manager**: Controls concurrent access to data
- **Buffer Manager**: Manages in-memory data pages

---

## Go Database Programming

### **Standard Library - `database/sql`**

```go
import (
    "database/sql"
    _ "github.com/lib/pq" // PostgreSQL driver
)

// Connection pattern
db, err := sql.Open("postgres", connectionString)
if err != nil {
    log.Fatal(err)
}
defer db.Close()
```

### **Key Types and Interfaces**

- **`sql.DB`**: Database connection pool
- **`sql.Tx`**: Database transaction
- **`sql.Stmt`**: Prepared statement
- **`sql.Rows`**: Result set from a query
- **`sql.Row`**: Single row result
- **`driver.Driver`**: Interface for database drivers

### **Connection Management**

- **Connection Pool**: Reusable database connections
- **`SetMaxOpenConns()`**: Maximum number of open connections
- **`SetMaxIdleConns()`**: Maximum number of idle connections
- **`SetConnMaxLifetime()`**: Maximum time a connection can be reused

---

## Storage Engine Concepts

### **Page-Based Storage**

- **Page**: Fixed-size block of data (typically 4KB, 8KB, or 16KB)
- **Page Header**: Metadata about the page (type, free space, etc.)
- **Page Directory**: Index of slots/records within a page
- **Slotted Page**: Page layout with directory pointing to variable-length records

```go
type Page struct {
    Header   PageHeader
    Data     []byte
    Slots    []SlotEntry
    FreePtr  uint16
}
```

### **File Organization**

- **Heap File**: Unordered collection of pages
- **Sorted File**: Pages maintained in sorted order
- **Hash File**: Pages organized using hash function
- **Clustered Index**: Data pages stored in index order

### **Record Management**

- **Fixed-Length Records**: All records have the same size
- **Variable-Length Records**: Records can have different sizes
- **Record ID (RID)**: Unique identifier for a record (page_id, slot_id)
- **Tombstone**: Marker indicating a deleted record

---

## Indexing and B-Trees

### **B-Tree Structure**

- **Node**: Basic unit of a B-tree (internal or leaf)
- **Degree (m)**: Maximum number of children per internal node
- **Order**: Minimum degree of the B-tree
- **Height**: Number of levels from root to leaves
- **Fanout**: Average number of children per internal node

```go
type BTreeNode struct {
    Keys     []Key
    Values   []Value
    Children []*BTreeNode
    IsLeaf   bool
    Parent   *BTreeNode
}
```

### **B-Tree Operations**

- **Search**: Finding a key in the tree
- **Insert**: Adding a new key-value pair
- **Delete**: Removing a key-value pair
- **Split**: Dividing an overfull node into two nodes
- **Merge**: Combining two underfull nodes
- **Redistribute**: Moving keys between sibling nodes

### **Index Types**

- **Primary Index**: Built on the primary key
- **Secondary Index**: Built on non-key attributes
- **Unique Index**: Ensures no duplicate values
- **Composite Index**: Built on multiple columns
- **Covering Index**: Contains all needed columns for a query

---

## Transaction Management

### **Concurrency Control**

- **Lock-Based**: Uses locks to control access
- **Optimistic**: Assumes conflicts are rare
- **Multiversion**: Maintains multiple versions of data
- **Timestamp Ordering**: Uses timestamps to order transactions

### **Lock Types**

- **Shared Lock (S)**: Multiple readers allowed
- **Exclusive Lock (X)**: Only one writer allowed
- **Intent Locks**: Indicate intention to lock at finer granularity
- **Deadlock**: Circular wait condition among transactions

### **Isolation Levels**

- **Read Uncommitted**: Dirty reads allowed
- **Read Committed**: Only committed data visible
- **Repeatable Read**: Same reads return same results
- **Serializable**: Equivalent to serial execution

```go
type Transaction struct {
    ID        TxnID
    State     TxnState
    Locks     map[ResourceID]LockType
    StartTime time.Time
    Operations []Operation
}
```

---

## Write-Ahead Logging (WAL)

### **WAL Concepts**

- **Log Record**: Entry describing a database operation
- **LSN (Log Sequence Number)**: Unique identifier for log records
- **Checkpoint**: Point where all dirty pages are written to disk
- **Log Buffer**: In-memory buffer for log records before disk write

### **Recovery Process**

- **Analysis Phase**: Scans log to find dirty pages and active transactions
- **Redo Phase**: Reapplies all logged operations
- **Undo Phase**: Rolls back uncommitted transactions

```go
type LogRecord struct {
    LSN       LogSequenceNumber
    TxnID     TransactionID
    Type      LogRecordType
    PageID    PageID
    Offset    uint16
    OldValue  []byte
    NewValue  []byte
    PrevLSN   LogSequenceNumber
}
```

### **WAL Implementation**

- **Force Policy**: Log records must be written before data pages
- **No-Steal Policy**: Uncommitted changes not written to disk
- **Group Commit**: Batch multiple transaction commits

---

## Memory Management

### **Buffer Pool Management**

- **Frame**: In-memory storage for a page
- **Dirty Bit**: Indicates if page has been modified
- **Pin Count**: Number of active references to a page
- **Replacement Policy**: Algorithm for choosing pages to evict

### **Replacement Algorithms**

- **LRU (Least Recently Used)**: Evict least recently accessed page
- **Clock**: Circular buffer with reference bits
- **LFU (Least Frequently Used)**: Evict least frequently accessed page
- **Random**: Random page selection for eviction

```go
type BufferPool struct {
    frames     []Frame
    freeList   []int
    hashTable  map[PageID]int
    replacer   Replacer
    diskManager DiskManager
}
```

---

## Concurrency Patterns

### **Go Concurrency Primitives**

- **`sync.Mutex`**: Mutual exclusion lock
- **`sync.RWMutex`**: Read-write mutex allowing multiple readers
- **`sync.WaitGroup`**: Waits for collection of goroutines to finish
- **`sync.Once`**: Ensures function is called only once
- **`sync.Cond`**: Condition variable for waiting/signaling

### **Channel Patterns**

```go
// Worker pool pattern
jobs := make(chan Job, 100)
results := make(chan Result, 100)

// Fan-out/Fan-in pattern
func fanOut(input <-chan int, workers int) []<-chan int {
    outputs := make([]<-chan int, workers)
    for i := 0; i < workers; i++ {
        output := make(chan int)
        outputs[i] = output
        go worker(input, output)
    }
    return outputs
}
```

### **Database-Specific Patterns**

- **Connection Pool**: Manage database connections across goroutines
- **Transaction Isolation**: Ensure transactions don't interfere
- **Lock Manager**: Coordinate access to shared resources
- **Background Tasks**: Checkpoint writing, log cleaning, statistics gathering

---

## Testing and Benchmarking

### **Unit Testing**

```go
func TestBTreeInsert(t *testing.T) {
    tree := NewBTree(3)
    tree.Insert(10, "value10")

    value, found := tree.Search(10)
    if !found {
        t.Errorf("Expected to find key 10")
    }
    if value != "value10" {
        t.Errorf("Expected value10, got %v", value)
    }
}
```

### **Benchmarking**

```go
func BenchmarkBTreeInsert(b *testing.B) {
    tree := NewBTree(100)
    b.ResetTimer()

    for i := 0; i < b.N; i++ {
        tree.Insert(i, fmt.Sprintf("value%d", i))
    }
}
```

### **Property-Based Testing**

- **Invariants**: Properties that should always hold
- **Fuzzing**: Testing with random inputs
- **Concurrency Testing**: Race condition detection

### **Database Testing Strategies**

- **Transaction Isolation Testing**: Verify isolation levels work correctly
- **Recovery Testing**: Ensure database recovers correctly after crashes
- **Performance Testing**: Measure throughput and latency under load
- **Stress Testing**: Test behavior under extreme conditions

---

## Key Performance Considerations

### **I/O Optimization**

- **Sequential vs Random Access**: Sequential I/O is much faster
- **Page Size**: Larger pages reduce I/O operations but increase memory usage
- **Prefetching**: Reading ahead to reduce future I/O operations
- **Batch Operations**: Group multiple operations to reduce overhead

### **CPU Optimization**

- **Cache Locality**: Keep related data close in memory
- **Branch Prediction**: Minimize unpredictable branches
- **SIMD Instructions**: Use vector operations for bulk processing
- **Hot/Cold Data Separation**: Keep frequently accessed data separate

### **Memory Optimization**

- **Object Pooling**: Reuse objects to reduce GC pressure
- **Memory Layout**: Optimize struct field ordering for cache efficiency
- **Zero-Copy Operations**: Avoid unnecessary data copying
- **Batch Allocation**: Allocate memory in batches to reduce fragmentation

---

## Common Database File Formats

### **Page Layout Types**

- **Slotted Page**: Variable-length records with slot directory
- **Fixed-Length Records**: Simple array-like layout
- **PAX (Partition Attributes Across)**: Hybrid row/column storage
- **NSM (N-ary Storage Model)**: Traditional row-oriented storage

### **Index File Formats**

- **B+Tree Files**: Leaf nodes contain all data, internal nodes only keys
- **Hash Index Files**: Hash-based key-value storage
- **Bitmap Index Files**: Bit vectors for set membership queries
- **Inverted Index Files**: Full-text search indexes

---

## Error Handling Best Practices

### **Database-Specific Errors**

```go
type DatabaseError struct {
    Code    ErrorCode
    Message string
    Cause   error
}

func (e *DatabaseError) Error() string {
    return fmt.Sprintf("DB Error %d: %s", e.Code, e.Message)
}

func (e *DatabaseError) Unwrap() error {
    return e.Cause
}
```

### **Transaction Error Handling**

- **Rollback on Error**: Always rollback failed transactions
- **Deadlock Detection**: Detect and handle deadlock situations
- **Timeout Handling**: Set reasonable timeouts for operations
- **Retry Logic**: Implement exponential backoff for transient failures

---

This reference covers the essential concepts you'll encounter when building database systems in Go. Keep this handy as you develop GengarDB, and refer back to specific sections as needed for your implementation.
