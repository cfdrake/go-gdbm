package gdbm

import (
	"os"
	"testing"
)

// Test that GDBM version is returned
func TestVersion(t *testing.T) {
	vers := Version()
	if len(vers) == 0 {
		t.Error("version string is not present")
	}
}

/* returns true if str is a word in list */
func ListContains(list []string, str string) bool {
	for i := range list {
		if list[i] == str {
			return true
		}
	}
	return false
}

// Create a database, insert two rows, and check that FirstKey, and NextKey return those keys
func TestKeys(t *testing.T) {
	var db_filename string = "test.gdbm" // pending the test_cleanup merge

	os.Remove(db_filename) // pending the test_cleanup merge
	db, err := Open(db_filename, "c")
	if err != nil {
		t.Error("Couldn't create new database")
	}
	defer db.Close()
	defer os.Remove(db_filename)

	err = db.Insert("foo", "bar")
	if err != nil {
		t.Error("Database let readonly client write")
	}
	err = db.Insert("baz", "bax")
	if err != nil {
		t.Error("Database let readonly client write")
	}
	err = db.Insert("biff", "bixx")
	if err != nil {
		t.Error("Database let readonly client write")
	}

	expected_keys := []string{
		"foo",
		"baz",
		"biff",
	}

	k, err := db.FirstKey()
	if err != nil {
		t.Error(err)
	}
	if !ListContains(expected_keys, k) {
		t.Errorf("FirstKey() expected: %s", expected_keys)
	}

	for i := 1; i < len(expected_keys); i++ {
		n, err := db.NextKey(k)
		if err != nil {
			t.Error(err)
		}
		if !ListContains(expected_keys, n) {
			t.Errorf("NextKey() expected: %s", expected_keys)
		}
	}

}

// Tests that the database is recreated everytime when opened in "c" mode.
// Ensures that the file exists and that there are no key-value pairs.
func TestRecreate(t *testing.T) {
	db, err := Open("test.gdbm", "c")
	db.Close() // pending the test_cleanup merge

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

// Ensure that the database doesn't allow a Reader type to call an database-
// modifying operation such as Insert().
func TestWriteErrorWhenReader(t *testing.T) {
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

// Tests to make sure that inserting a key, then replacing its old value with a
// new one will not return an error (as would happen if you were to call
// Insert() again rather than Replace()) and that the key is actually updated.
func TestReplace(t *testing.T) {
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

// Ensures that if a key k is inserted into the database, calling Exists(k)
// returns true - indicating that the insertion was successful.
func TestExists(t *testing.T) {
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

// Test to make sure that if a key is inserted then deleted, that it no longer
// returns true for Exists(key).
func TestDelete(t *testing.T) {
	db, err := Open("test.gdbm", "c")
	defer db.Close()

	db.Insert("foo", "bar")
	err = db.Delete("foo")
	exists := db.Exists("foo")
	if err != nil || exists {
		t.Error("Delete()ed key not removed")
	}
}

// Ensure the Reorganize() function doesn't halt execution.
func TestReorganize(t *testing.T) {
	// No values to test, but testing as a sanity test - if something does go
	// wrong and the program crashes, this'll let us know Reorganize() is
	// broken. ;)
	db, _ := Open("test.gdbm", "c")
	defer db.Close()
	db.Reorganize()
}

// Ensure the Sync() function doesn't halt execution.
func TestSync(t *testing.T) {
	// Same as TestReorganize() w/r/t no return value, etc.
	db, _ := Open("test.gdbm", "c")
	defer db.Close()
	db.Sync()
}
