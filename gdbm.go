// Packge gdbm implements a wrapper around libgdbm, the GNU DataBase Manager
// library, for Go.
package gdbm

// #cgo CFLAGS: -std=gnu99
// #cgo LDFLAGS: -lgdbm
// #include <stdlib.h>
// #include <gdbm.h>
// #include <string.h>
// inline datum mk_datum(char * s) {
//     datum d;
//     d.dptr = s;
//     d.dsize = strlen(s);
//     return d;
// }
import "C"

import (
	"errors"
	"unsafe"
)

// GDBM database "connection" type.
type Database struct {
	dbf  C.GDBM_FILE
	mode int
}

// GDBM database configuration type that can be used to specify how the
// database is created and treated. The `Mode` determines what the user of the
// `Database` object can do with it. The field can be a string, either "r" for
// Read-only, "w" for Read-Write, "c" for Read-Write and create if it doesn't
// exist, and "n" for Read-Write and always recreate database, even if it
// exists. TODO: write descriptions for other params... too lazy right now
type DatabaseCfg struct {
	Mode        string
	BlockSize   int
	Permissions int
}

func lastError() error {
	return errors.New(C.GoString(C.gdbm_strerror(C.gdbm_errno)))
}

// Simple function to open a database file with default parameters (block size
// is default for the filesystem and file permissions are set to 0666).
func Open(filename string, mode string) (db *Database, err error) {
	return OpenWithCfg(filename, DatabaseCfg{mode, 0, 0666})
}

// More complex database initialization function that takes in a `DatabaseCfg`
// struct to allow more fine-grained control over database settings.
func OpenWithCfg(filename string, cfg DatabaseCfg) (db *Database, err error) {
	db = new(Database)

	// Convert a human-readable mode string into a libgdbm-usable constant.
	switch cfg.Mode {
	case "r":
		db.mode = C.GDBM_READER
	case "w":
		db.mode = C.GDBM_WRITER
	case "c":
		db.mode = C.GDBM_WRCREAT
	case "n":
		db.mode = C.GDBM_NEWDB
	}

	cs := C.CString(filename)
	defer C.free(unsafe.Pointer(cs))
	db.dbf = C.gdbm_open(cs, C.int(cfg.BlockSize), C.int(db.mode), C.int(cfg.Permissions), nil)
	if db.dbf == nil {
		err = lastError()
	}
	return db, err
}

// Closes a database's internal file pointer.
func (db *Database) Close() {
	C.gdbm_close(db.dbf)
}

// Internal helper method to hide the two constants GDBM_INSERT and
// GDBM_REPLACE from the user.
func (db *Database) update(key string, value string, flag C.int) (err error) {
	// Convert key and value into libgdbm's `datum` data structure. See the
	// C definition at the top for the implementation of C.mk_datum(string).
	kcs := C.CString(key)
	vcs := C.CString(value)
	k := C.mk_datum(kcs)
	v := C.mk_datum(vcs)
	defer C.free(unsafe.Pointer(kcs))
	defer C.free(unsafe.Pointer(vcs))

	retv := C.gdbm_store(db.dbf, k, v, flag)
	if retv != 0 {
		err = lastError()
	}
	return err
}

// Inserts a key-value pair into the database. If the database is opened
// in "r" mode, this will return an error. Also, if the key already exists in
// the database, and error will be returned.
func (db *Database) Insert(key string, value string) (err error) {
	return db.update(key, value, C.GDBM_INSERT)
}

// Updates a key-value pair to use a new value, specified by the `value` string
// parameter. An error will be returned if the database is opened in "r" mode.
func (db *Database) Replace(key string, value string) (err error) {
	return db.update(key, value, C.GDBM_REPLACE)
}

// Returns true or false, depending on whether the specified key exists in the
// database.
func (db *Database) Exists(key string) bool {
	kcs := C.CString(key)
	k := C.mk_datum(kcs)
	defer C.free(unsafe.Pointer(kcs))

	e := C.gdbm_exists(db.dbf, k)
	if e == 1 {
		return true
	}
	return false
}

// Returns the firstkey in this gdbm.Database. If there is not a key, an
// error will be returned in err.
func (db *Database) FirstKey() (value string, err error) {
	vdatum := C.gdbm_firstkey(db.dbf)
	if vdatum.dptr == nil {
		return "", lastError()
	}

	value = C.GoString(vdatum.dptr)
	defer C.free(unsafe.Pointer(vdatum.dptr))
	return value, nil
}

// Returns the nextkey after `key`. If there is not a next key, an
// error will be returned in err.
func (db *Database) NextKey(key string) (value string, err error) {
	kcs := C.CString(key)
	k := C.mk_datum(kcs)
	defer C.free(unsafe.Pointer(kcs))

	vdatum := C.gdbm_nextkey(db.dbf, k)
	if vdatum.dptr == nil {
		return "", lastError()
	}

	value = C.GoString(vdatum.dptr)
	defer C.free(unsafe.Pointer(vdatum.dptr))
	return value, nil
}

// return a map of [key]value, otherwise `err`
func (db *Database) ToMap() (db_map map[string]string, err error) {
	var (
		curr_k string
		curr_v string
		next_k string
		next_v string
	)
	db_map = make(map[string]string)

	curr_k, err = db.FirstKey()
	if err != nil {
		return db, nil
	}

	curr_v, err = db.Fetch(curr_k)
	if err != nil {
		return db, nill
	}

	db_map[curr_k] = curr_v

	for {
		next_k, err = db.NextKey(curr_k)
		if err != nil {
			break
		}

		next_v, err = db.Fetch(next_k)
		if err != nil {
			break
		}
		db_map[next_k] = next_v

		curr_k = next_k
	}
	return db_map, nil
}

// Fetches the value of the given key. If the key is not in the database, an
// error will be returned in err. Otherwise, value will be the value string
// that is keyed by `key`.
func (db *Database) Fetch(key string) (value string, err error) {
	kcs := C.CString(key)
	k := C.mk_datum(kcs)
	defer C.free(unsafe.Pointer(kcs))

	vdatum := C.gdbm_fetch(db.dbf, k)
	if vdatum.dptr == nil {
		return "", lastError()
	}

	value = C.GoString(vdatum.dptr)
	defer C.free(unsafe.Pointer(vdatum.dptr))
	return value, nil
}

// Removes a key-value pair from the database. If the database is opened in "r"
// mode, an error is returned
func (db *Database) Delete(key string) (err error) {
	kcs := C.CString(key)
	k := C.mk_datum(kcs)
	defer C.free(unsafe.Pointer(kcs))

	retv := C.gdbm_delete(db.dbf, k)
	if retv == -1 && db.mode == C.GDBM_READER {
		err = lastError()
	}
	return err
}

// Reorganizes the database for more efficient use of disk space. This method
// can be used if Delete(k) is called many times.
func (db *Database) Reorganize() {
	C.gdbm_reorganize(db.dbf)
}

// Synchronizes all pending database changes to the disk. TODO: note this is
// only needed in FAST mode, and FAST mode needs implemented!
func (db *Database) Sync() {
	C.gdbm_sync(db.dbf)
}
