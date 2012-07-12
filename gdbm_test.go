package gdbm

import (
    "testing"
)

func TestCreate(t * testing.T) {
    db, err := Open("test.gdbm", "c")
    defer db.Close()
    if err != nil {
        t.Error("Couldn't create new database")
    }
}
