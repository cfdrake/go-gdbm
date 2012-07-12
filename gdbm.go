//
//
//
//

package gdbm

// #cgo CFLAGS: -std=gnu99
// #cgo LDFLAGS: -lgdbm
// #include <stdlib.h>
// #include <gdbm.h>
import "C"

import (
    "errors"
    "unsafe"
)

var errText = map[int]string{ 0: "",
}

func lastError() error {
    return errors.New(C.GoString(C.gdbm_strerror(C.gdbm_errno)))
}

//
type Database struct {
    dbf C.GDBM_FILE
}

//
type DatabaseCfg struct {
    Mode string
    BlockSize int
    Permissions int
}

// Simple function to open a database file with default parameters (block size
// is default for the filesystem and file permissions are set to 0666).
func Open(filename string, mode string) (db * Database, err error) {
    return OpenWithCfg(filename, DatabaseCfg{mode, 0, 0666})
}

func OpenWithCfg(filename string, cfg DatabaseCfg) (db * Database, err error) {
    var m int
    switch cfg.Mode {
    case "r": m = C.GDBM_READER
    case "w": m = C.GDBM_WRITER
    case "c": m = C.GDBM_WRCREAT
    case "n": m = C.GDBM_NEWDB
    }

    cs := C.CString(filename)
    defer C.free(unsafe.Pointer(cs))

    db.dbf = C.gdbm_open(cs, C.int(cfg.BlockSize), C.int(m), C.int(cfg.Permissions), nil)
    if db.dbf == nil {
        err = lastError()
    }
    return db, err
}

// Closes a database file.
func (db * Database) Close() {
    C.gdbm_close(db.dbf)
}

