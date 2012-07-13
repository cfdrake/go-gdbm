//
//
//
//

package gdbm

import (
    "os"
    "testing"
)

// Tests that the database is recreated everytime when opened in "c" mode.
// Ensures that the file exists and that there are no key-value pairs.
func TestRecreate(t * testing.T) {
    db, err := Open("test.gdbm", "c")
    defer db.Close()

    if err != nil {
        t.Error("Couldn't create new database")
    }

    f, err := Open("test.gdbm", "r")
    defer f.Close()

    if os.IsExist(err) {
        t.Error("Database wasn't actually created")
    }

    // TODO: test for no keys
}

// TODO: test other initializers

//
func TestWriteErrorWhenReader(t * testing.T) {
    // Ensure the db exists for this test
    db, err := Open("test.gdbm", "c")
    db.Close()

    db, err = Open("test.gdbm", "r")
    defer db.Close()

    if err != nil {
        t.Error("Couldn't read database")
    }

    err = db.Insert("foo", "bar")
    if err == nil {
        t.Error("Database let readonly client write")
    }
}

//
func TestReplace(t * testing.T) {
    db, err := Open("test.gdbm", "c")
    defer db.Close()

    if err != nil {
        t.Error("Couldn't create database")
    }

    err = db.Insert("foo", "bar")
    err = db.Replace("foo", "biz")
    key, err := db.Fetch("foo")
    if err != nil || key != "biz" {
        t.Error("Replace didn't update key correctly")
    }
}

//
func TestExists(t * testing.T) {
    db, err := Open("test.gdbm", "c")
    defer db.Close()

    if err != nil {
        t.Error("Couldn't create database")
    }

    err = db.Insert("foo", "bar")
    exists := db.Exists("foo")
    if !exists {
        t.Error("Inserted key reported as not existing")
    }
}

//
func TestDelete(t * testing.T) {
    db, err := Open("test.gdbm", "c")
    defer db.Close()

    db.Insert("foo", "bar")
    err = db.Delete("foo")
    exists := db.Exists("foo")
    if err != nil || exists {
        t.Error("Delete()ed key not removed")
    }
}

//
func TestReorganize(t * testing.T) {
    // No values to test, but testing as a sanity test - if something does go
    // wrong and the program crashes, this'll let us know Reorganize() is
    // broken ;)
    db, _ := Open("test.gdbm", "c")
    defer db.Close()
    db.Reorganize()
}

//
func TestSync(t * testing.T) {
    // Same as TestReorganize() w/r/t no return value, etc.
    db, _ := Open("test.gdbm", "c")
    defer db.Close()
    db.Sync()
}
