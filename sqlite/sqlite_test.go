package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/chmike/migrate"
)

// createSteps creates two sqlite migration steps.
func createSteps() *migrate.Steps {
	s := migrate.NewSteps("test database")

	// v1
	s.Append("create table",
		Tx(Cmd(`CREATE TABLE "test" ("id" INTEGER PRIMARY KEY AUTOINCREMENT, "msg" TEXT NOT NULL);`)),
		NoTxF(func(ctx context.Context, db SQLDB, info StepInfo, log Logger) error {
			_, err := db.DB().ExecContext(ctx, `DROP TABLE "test";`)
			return err
		}),
	)

	// v2
	s.Append("insert row",
		TxF(func(tx SQLTx, info StepInfo, dryRun bool, log Logger) error {
			_, err := tx.Tx().Exec(`INSERT INTO "test" ("msg") VALUES (?);`,
				fmt.Sprintf("test: %s: %v -> %v", info.Name(), info.From(), info.To()))
			return err
		}),
		NoTx(Cmd(`DELETE FROM "test";`)),
	)
	return s
}

// getSQLiteTables returns the list of tables in an sqlite database.
func getSQLiteTables(db *sql.DB) ([]string, error) {
	var tables []string

	query := "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		tables = append(tables, tableName)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tables, nil
}

func TestSqlOpenErrors(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "sqlite_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	forceSqlOpenError = errors.New("sqlite open error")
	fileName := filepath.Join(tempDir, "data.db")
	_, err = Open(fileName)
	if err == nil {
		t.Fatal(err)
	}
	forceSqlOpenError = nil

	_, err = Open(fileName, WithTableName("table with space"))
	if err == nil {
		t.Fatal(err)
	}
}

func TestSqliteOpen(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "sqlite_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	fileName := filepath.Join(tempDir, "data.db")
	db, err := Open(fileName)
	if err != nil {
		t.Fatal(err)
	}
	defer db.DB().Close()

	s := createSteps()

	m, err := NewMigrator(db, s, nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := m.Init(); err != nil {
		t.Fatal(err)
	}

	tables, err := getSQLiteTables(db.DB())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(tables, "migrate_version") {
		t.Fatalf("expect migrate_version in %v", tables)
	}

	if err := m.OneUpDryRun(); err != nil {
		t.Fatal(err)
	}
	tables, err = getSQLiteTables(db.DB())
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != 1 {
		t.Fatalf("expect one table in %v", tables)
	}
	if !slices.Contains(tables, "migrate_version") {
		t.Fatalf("expect migrate_version in %v", tables)
	}

	if err := m.OneUp(); err != nil {
		t.Fatal(err)
	}
	tables, err = getSQLiteTables(db.DB())
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != 2 {
		t.Fatalf("expect two table in %v", tables)
	}
	if !slices.Contains(tables, "migrate_version") {
		t.Fatalf("expect migrate_version in %v", tables)
	}
	if !slices.Contains(tables, "test") {
		t.Fatalf("expect migrate_version in %v", tables)
	}

	if err := m.AllUp(); err != nil {
		t.Fatal(err)
	}

	v, err := m.Version()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("all up version is %v", v)

	if err := m.OneDown(); err != nil {
		t.Fatal(err)
	}

	v, err = m.Version()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("all up version is %v", v)

	if err := m.AllDown(); err != nil {
		t.Fatal(err)
	}
	tables, err = getSQLiteTables(db.DB())
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != 1 {
		t.Fatalf("expect one table in %v", tables)
	}
	if !slices.Contains(tables, "migrate_version") {
		t.Fatalf("expect migrate_version in %v", tables)
	}
}

func TestSqliteOpenTable(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "sqlite_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	tableName := "temp_version"

	fileName := filepath.Join(tempDir, "data.db")
	db, err := Open(fileName, WithTableName(tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.DB().Close()

	s := createSteps()

	m, err := NewMigrator(db, s, nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := m.Init(); err != nil {
		t.Fatal(err)
	}

	tables, err := getSQLiteTables(db.DB())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(tables, tableName) {
		t.Fatalf("expect %v in %v", tableName, tables)
	}

	if err := m.OneUpDryRun(); err != nil {
		t.Fatal(err)
	}
	tables, err = getSQLiteTables(db.DB())
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != 1 {
		t.Fatalf("expect one table in %v", tables)
	}
	if !slices.Contains(tables, tableName) {
		t.Fatalf("expect %v in %v", tableName, tables)
	}

	if err := m.OneUp(); err != nil {
		t.Fatal(err)
	}
	tables, err = getSQLiteTables(db.DB())
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != 2 {
		t.Fatalf("expect two table in %v", tables)
	}
	if !slices.Contains(tables, tableName) {
		t.Fatalf("expect %v in %v", tableName, tables)
	}
	if !slices.Contains(tables, "test") {
		t.Fatalf("expect test in %v", tables)
	}

	if err := m.AllUp(); err != nil {
		t.Fatal(err)
	}

	v, err := m.Version()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("all up version is %v", v)

	if err := m.OneDown(); err != nil {
		t.Fatal(err)
	}

	v, err = m.Version()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("all up version is %v", v)

	if err := m.AllDown(); err != nil {
		t.Fatal(err)
	}
	tables, err = getSQLiteTables(db.DB())
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != 1 {
		t.Fatalf("expect one table in %v", tables)
	}
	if !slices.Contains(tables, tableName) {
		t.Fatalf("expect %v in %v", tableName, tables)
	}
}

// func TestSqliteOpenErrors(t *testing.T) {
// 	_, err := Open("broken.db")
// 	if err == nil {
// 		t.Fatal("expect error")
// 	}

// 	_, err = Open(":memory:", WithTableName("space not allowed"))
// 	if err == nil {
// 		t.Fatal("expect error")
// 	}

// 	// defer db.DB().Close()
// }

func TestFixedBrokenSqliteOpen(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "sqlite_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	t.Run("CreateOnly_Success", func(t *testing.T) {
		dbPath := filepath.Join(tempDir, "test_create_only.db")
		db, err := fixedBrokenSqliteOpen(dbPath, createOnly)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		if _, err := os.Stat(dbPath); errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	})

	t.Run("CreateOnly_FileExists", func(t *testing.T) {
		dbPath := filepath.Join(tempDir, "test_exists.db")

		createValidSqliteFile(t, dbPath)

		_, err := fixedBrokenSqliteOpen(dbPath, createOnly)
		if !errors.Is(errors.Unwrap(err), os.ErrExist) {
			t.Fatalf("Expected os.ErrExist, got: %v", err)
		}
	})

	t.Run("OpenOnly_Success", func(t *testing.T) {
		dbPath := filepath.Join(tempDir, "test_open_only.db")

		createValidSqliteFile(t, dbPath)

		db, err := fixedBrokenSqliteOpen(dbPath, openOnly)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
	})

	t.Run("OpenOnly_FileNotExists", func(t *testing.T) {
		dbPath := filepath.Join(tempDir, "nonexistent.db")
		_, err := fixedBrokenSqliteOpen(dbPath, openOnly)
		if !errors.Is(errors.Unwrap(err), os.ErrNotExist) {
			t.Fatalf("Expected os.ErrNotExist, got: %v", err)
		}
	})

	t.Run("CreateOrOpen_Existing", func(t *testing.T) {
		dbPath := filepath.Join(tempDir, "test_create_or_open.db")

		createValidSqliteFile(t, dbPath)

		db, err := fixedBrokenSqliteOpen(dbPath, createOrOpen)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
	})

	t.Run("CreateOrOpen_New", func(t *testing.T) {
		dbPath := filepath.Join(tempDir, "test_create_or_open_new.db")
		db, err := fixedBrokenSqliteOpen(dbPath, createOrOpen)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		if _, err := os.Stat(dbPath); errors.Is(err, os.ErrNotExist) {
			t.Fatal("file not created")
		}
	})

	t.Run("NotARegularFile", func(t *testing.T) {
		dirPath := filepath.Join(tempDir, "dir_not_file")
		err := os.Mkdir(dirPath, 0755)
		if err != nil {
			t.Fatal(err)
		}

		_, err = fixedBrokenSqliteOpen(dirPath, openOnly)
		if err == nil {
			t.Fatal("expected error")
		}
		if err.Error() != dirPath+" is not a file" {
			t.Fatal(err)
		}
	})

	t.Run("InvalidSqliteFile", func(t *testing.T) {
		invalidFilePath := filepath.Join(tempDir, "invalid.db")

		err := os.WriteFile(invalidFilePath, []byte("This is not a SQLite file"), 0644)
		if err != nil {
			t.Fatal(err)
		}

		_, err = fixedBrokenSqliteOpen(invalidFilePath, openOnly)
		if err == nil {
			t.Fatal("unexpected error")
		}
	})

	if os.Getuid() != 0 {
		t.Run("NoWritePermission", func(t *testing.T) {
			readOnlyDir := filepath.Join(tempDir, "readonly")
			err := os.Mkdir(readOnlyDir, 0500) // r-x------
			if err != nil {
				t.Fatalf("failed creating read only folder: %v", err)
			}

			dbPath := filepath.Join(readOnlyDir, "test.db")
			_, err = fixedBrokenSqliteOpen(dbPath, createOrOpen)
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}

	t.Run("Sql open error", func(t *testing.T) {
		mockDbPath := filepath.Join(tempDir, "mock_error.db")
		createValidSqliteFile(t, mockDbPath)

		forceSqlOpenError = errors.New("force sql open error")
		db, err := fixedBrokenSqliteOpen(mockDbPath, openOnly)
		if err == nil {
			db.Close()
			t.Fatal("expect error")
		}
		forceSqlOpenError = nil
	})

	t.Run("Sql ping error", func(t *testing.T) {
		mockDbPath := filepath.Join(tempDir, "mock_error.db")
		createValidSqliteFile(t, mockDbPath)

		// corrupt the sqlite database
		f, err := os.OpenFile(mockDbPath, os.O_WRONLY, 0644)
		if err != nil {
			t.Fatal(err)
		}
		f.Seek(20, 0)
		_, err = f.Write(bytes.Repeat([]byte{0xFF}, 100))
		if err != nil {
			t.Fatal(err)
		}
		f.Close()
		if err != nil {
			t.Fatalf("failed to corrupt file: %v", err)
		}
		db, err := fixedBrokenSqliteOpen(mockDbPath, openOnly)
		if err == nil {
			db.Close()
			t.Fatal("expect error")
		}
	})
}

func TestCheckSQLiteHeader(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "sqlite_header_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	t.Run("ValidHeader", func(t *testing.T) {
		validPath := filepath.Join(tempDir, "valid.db")
		createValidSqliteFile(t, validPath)

		err := checkSQLiteHeader(validPath)
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("InvalidHeader", func(t *testing.T) {
		invalidPath := filepath.Join(tempDir, "invalid.db")
		err := os.WriteFile(invalidPath, []byte("Not a SQLite DB"), 0644)
		if err != nil {
			t.Fatal(err)
		}

		err = checkSQLiteHeader(invalidPath)
		if err == nil {
			t.Fatal("expect error")
		}
		if err.Error() != "bad sqlite3 file signature" {
			t.Fatal(err)
		}
	})

	t.Run("FileNotExists", func(t *testing.T) {
		nonexistentPath := filepath.Join(tempDir, "nonexistent.db")

		err := checkSQLiteHeader(nonexistentPath)
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("Expected os.ErrNotExist, got: %v", err)
		}
	})

	t.Run("FileTooShort", func(t *testing.T) {
		shortPath := filepath.Join(tempDir, "short.db")
		err := os.WriteFile(shortPath, []byte("SQLite"), 0644)
		if err != nil {
			t.Fatal(err)
		}

		err = checkSQLiteHeader(shortPath)
		if err == nil {
			t.Fatal("expect error")
		}
	})

	t.Run("Read error", func(t *testing.T) {
		mockDbPath := filepath.Join(tempDir, "mock_error.db")
		os.Remove(mockDbPath)
		createValidSqliteFile(t, mockDbPath)
		forceReadError = errors.New("fake read error")
		err := checkSQLiteHeader(mockDbPath)
		os.Remove(mockDbPath)
		if err != forceReadError {
			t.Fatal("expect fake read error")
		}
		forceReadError = nil
	})
}

func createValidSqliteFile(t *testing.T, path string) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec("PRAGMA user_version = 1;")
	if err != nil {
		t.Fatal(err)
	}
}
