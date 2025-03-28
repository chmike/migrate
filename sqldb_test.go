package migrate

import (
	"context"
	"encoding/hex"
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

type customResult struct {
	lastID      int64
	lastIdErr   error
	nRows       int64
	affectedErr error
}

var mockQ = &Queries{
	CreateTableQuery: `CREATE TABLE "migrate_version" ("id" INTEGER NOT NULL, "checksum" TEXT NOT NULL)`,
	InitTableQuery:   `INSERT INTO "migrate_version" ("id", "checksum") VALUES (?, ?)`,
	VersionQuery:     `SELECT "id", "checksum" FROM "migrate_version" LIMIT 1`,
	SetVersionQuery:  `UPDATE "migrate_version" SET "id" = ?, "checksum" = ? WHERE "id" = ? AND "checksum" = ?`,
}

func (r customResult) LastInsertId() (int64, error) {
	return r.lastID, r.lastIdErr
}

func (r customResult) RowsAffected() (int64, error) {
	return r.nRows, r.affectedErr
}

func TestQueries(t *testing.T) {
	expQ := &Queries{
		CreateTableQuery: `CREATE TABLE "test_table" ("id" INTEGER NOT NULL, "checksum" TEXT NOT NULL)`,
		InitTableQuery:   `INSERT INTO "test_table" ("id", "checksum") VALUES (?, ?)`,
		VersionQuery:     `SELECT "id", "checksum" FROM "test_table" LIMIT 1`,
		SetVersionQuery:  `UPDATE "test_table" SET "id" = ?, "checksum" = ? WHERE "id" = ? AND "checksum" = ?`,
	}

	resQ := &Queries{
		CreateTableQuery: `CREATE TABLE "migrate_version" ("id" INTEGER NOT NULL, "checksum" TEXT NOT NULL)`,
		InitTableQuery:   `INSERT INTO "migrate_version" ("id", "checksum") VALUES (?, ?)`,
		VersionQuery:     `SELECT "id", "checksum" FROM "migrate_version" LIMIT 1`,
		SetVersionQuery:  `UPDATE "migrate_version" SET "id" = ?, "checksum" = ? WHERE "id" = ? AND "checksum" = ?`,
	}
	resQ.Replace("migrate_version", "test_table")

	if *resQ != *expQ {
		t.Fatalf("expect equal")
	}
}

func TestSQLCommand(t *testing.T) {
	var c = SQLCommand{Cmd: "command string", Args: []any{"test", 123}}
	if exp := "`command string` args:[test, 123]"; c.String() != exp {
		t.Fatalf("expect %q, got %q", exp, c.String())
	}

	c = SQLCommand{Cmd: "command string"}
	if exp := "command string"; c.String() != exp {
		t.Fatalf("expect %q, got %q", exp, c.String())
	}
}

func TestStartTransaction(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}

	db := NewSQLDB(mockDB, mockQ)
	ctx := context.Background()
	defer mockDB.Close()

	if db.Queries() != mockQ {
		t.Fatalf("unexpected mismatch")
	}

	t.Run("Success", func(t *testing.T) {
		mock.ExpectBegin()

		tx, err := db.StartTransaction(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if tx == nil {
			t.Fatal("unexpected nil value")
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Failure", func(t *testing.T) {
		expectedErr := errors.New("BeginTx error")
		mock.ExpectBegin().WillReturnError(expectedErr)

		tx, err := db.StartTransaction(ctx, nil)
		if err == nil {
			t.Fatal("expect error")
		}
		if !errors.Is(err, expectedErr) {
			t.Fatalf("expect %q, got %q", expectedErr, err)
		}
		if tx != nil {
			t.Fatal("expect nil value")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestFinalizeTransaction(t *testing.T) {
	// Create the mock of the database.
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("error while creating mock: %v", err)
	}
	defer mockDB.Close()
	db := NewSQLDB(mockDB, mockQ)

	t.Run("Commit success", func(t *testing.T) {
		mock.ExpectBegin()
		mock.ExpectCommit()

		tx, err := db.StartTransaction(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}

		tx.FinalizeTransaction(&err, false)
		if err != nil {
			t.Fatal(err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Commit failure", func(t *testing.T) {
		mock.ExpectBegin()
		commitErr := errors.New("commit error")
		mock.ExpectCommit().WillReturnError(commitErr)

		tx, err := db.StartTransaction(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}

		tx.FinalizeTransaction(&err, false)

		if err == nil {
			t.Fatal("expect error")
		}
		if !errors.Is(err, commitErr) {
			t.Fatalf("expect %q, got %q", commitErr, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Rollback due to error", func(t *testing.T) {
		mock.ExpectBegin()
		mock.ExpectRollback()

		tx, err := db.StartTransaction(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}

		initialErr := errors.New("initial error")
		err = initialErr

		tx.FinalizeTransaction(&err, false)

		if err == nil {
			t.Fatal("expect error")
		}
		if !errors.Is(err, initialErr) {
			t.Fatalf("expect %q, got %q", initialErr, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Rollback due to dryRun", func(t *testing.T) {
		mock.ExpectBegin()
		mock.ExpectRollback()

		tx, err := db.StartTransaction(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}

		tx.FinalizeTransaction(&err, true)

		if err != nil {
			t.Fatal(err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Rollback failure with initial error", func(t *testing.T) {
		mock.ExpectBegin()
		rollbackErr := errors.New("rollback error")
		mock.ExpectRollback().WillReturnError(rollbackErr)

		tx, err := db.StartTransaction(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		initialErr := errors.New("initial error")
		err = initialErr

		tx.FinalizeTransaction(&err, false)

		if err == nil {
			t.Fatal("expect error")
		}
		if !errors.Is(err, initialErr) {
			t.Fatalf("expect %q, got %q", initialErr, err)
		}
		if !errors.Is(err, rollbackErr) {
			t.Fatalf("expect %q, got %q", initialErr, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Rollback failure without initial error", func(t *testing.T) {
		mock.ExpectBegin()
		rollbackErr := errors.New("rollback error")
		mock.ExpectRollback().WillReturnError(rollbackErr)

		tx, err := db.StartTransaction(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}

		tx.FinalizeTransaction(&err, true)

		if err == nil {
			t.Fatal("expect error")
		} else if !errors.Is(err, ErrRollbackTx) {
			t.Fatalf("expect %v, got %v", ErrRollbackTx, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestInitVersion(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()
	db := NewSQLDB(mockDB, mockQ)

	testVersion := Version{
		ID:       123,
		Checksum: [32]byte{1, 2, 3, 4},
	}
	ctx := context.Background()

	tests := []struct {
		name        string
		setupMock   func(mock sqlmock.Sqlmock)
		dryRun      bool
		expectErr   bool
		errContains string
	}{
		{
			name: "Success",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(mockQ.CreateTableQuery)).WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectExec(regexp.QuoteMeta(mockQ.InitTableQuery)).
					WithArgs(testVersion.ID, hex.EncodeToString(testVersion.Checksum[:])).
					WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectCommit()
			},
			dryRun:    false,
			expectErr: false,
		},
		{
			name: "Begin transaction error",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin().WillReturnError(errors.New("connection error"))
			},
			dryRun:      false,
			expectErr:   true,
			errContains: "not initialized",
		},
		{
			name: "Exec create table error",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(mockQ.CreateTableQuery)).WillReturnError(errors.New("syntax error"))
				mock.ExpectRollback()
			},
			dryRun:      false,
			expectErr:   true,
			errContains: "syntax error",
		},
		{
			name: "Exec init table error",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(mockQ.CreateTableQuery)).WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectExec(regexp.QuoteMeta(mockQ.InitTableQuery)).
					WithArgs(testVersion.ID, hex.EncodeToString(testVersion.Checksum[:])).
					WillReturnError(errors.New("syntax error"))
				mock.ExpectRollback()
			},
			dryRun:      false,
			expectErr:   true,
			errContains: "syntax error",
		},
		{
			name: "Dry run",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(mockQ.CreateTableQuery)).WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectExec(regexp.QuoteMeta(mockQ.InitTableQuery)).
					WithArgs(testVersion.ID, hex.EncodeToString(testVersion.Checksum[:])).
					WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectRollback()
			},
			dryRun:    true,
			expectErr: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.setupMock(mock)

			err := db.InitVersion(ctx, testVersion, test.dryRun)

			if test.expectErr {
				if err == nil {
					t.Errorf("unexpected nil error")
				} else if test.errContains != "" && !strings.Contains(err.Error(), test.errContains) {
					t.Errorf("Expected error containing %q but got: %v", test.errContains, err)
				}
			} else if err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Expectations not met: %v", err)
			}
		})
	}
}

func TestSqldbVersion(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()
	db := NewSQLDB(mockDB, mockQ)

	testVersion := Version{
		ID:       123,
		Checksum: [32]byte{1, 2, 3, 4},
	}
	ctx := context.Background()

	tests := []struct {
		name        string
		setupMock   func(mock sqlmock.Sqlmock)
		dryRun      bool
		expectErr   bool
		errContains string
	}{
		{
			name: "Success",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				rows := sqlmock.NewRows([]string{"id", "checksum"}).
					AddRow(123, hex.EncodeToString(testVersion.Checksum[:]))
				mock.ExpectQuery(mockQ.VersionQuery).WillReturnRows(rows)
				mock.ExpectCommit()
			},
			dryRun:    false,
			expectErr: false,
		},
		{
			name: "Start transaction error",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin().WillReturnError(errors.New("connection error"))
			},
			expectErr:   true,
			errContains: "connection error",
		},
		{
			name: "Query error",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectQuery(mockQ.VersionQuery).WillReturnError(errors.New("database error"))
				mock.ExpectRollback()
			},
			expectErr:   true,
			errContains: "database error",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.setupMock(mock)

			_, err := db.Version(ctx)

			if test.expectErr {
				if err == nil {
					t.Errorf("unexpected nil error")
				} else if test.errContains != "" && !strings.Contains(err.Error(), test.errContains) {
					t.Errorf("Expected error containing %q but got: %v", test.errContains, err)
				}
			} else if err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Expectations not met: %v", err)
			}
		})
	}
}

func TestVersionTx(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()
	db := NewSQLDB(mockDB, mockQ)

	testVersion := Version{
		ID:       123,
		Checksum: [32]byte{1, 2, 3, 4},
	}
	ctx := context.Background()

	tests := []struct {
		name        string
		setupMock   func(mock sqlmock.Sqlmock)
		dryRun      bool
		expectErr   bool
		errContains string
	}{
		{
			name: "Success",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				rows := sqlmock.NewRows([]string{"id", "checksum"}).
					AddRow(123, hex.EncodeToString(testVersion.Checksum[:]))
				mock.ExpectQuery(mockQ.VersionQuery).
					WillReturnRows(rows)
				mock.ExpectCommit()
			},
			dryRun:    false,
			expectErr: false,
		},
		{
			name: "Query error",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectQuery(mockQ.VersionQuery).WillReturnError(errors.New("database error"))
				mock.ExpectCommit()
			},
			expectErr:   true,
			errContains: "database error",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.setupMock(mock)

			tx, err := db.StartTransaction(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}

			v, err := db.VersionTx(tx)

			var finalizeErr error
			tx.FinalizeTransaction(&finalizeErr, test.dryRun)
			if finalizeErr != nil {
				t.Fatal(finalizeErr)
			}

			if test.expectErr {
				if err == nil {
					t.Errorf("unexpected nil error")
				} else if test.errContains != "" && !strings.Contains(err.Error(), test.errContains) {
					t.Errorf("Expected error containing %q but got: %v", test.errContains, err)
				}
			} else if err != nil {
				t.Errorf("Expected no error but got: %v", err)
			} else if v != testVersion {
				t.Fatalf("expect %v, got %v", testVersion, v)
			}

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Expectations not met: %v", err)
			}
		})
	}
}

func TestSetVersion(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()
	db := NewSQLDB(mockDB, mockQ)

	v1 := Version{
		ID:       100,
		Checksum: [32]byte{1, 2, 3, 4},
	}
	v2 := Version{
		ID:       123,
		Checksum: [32]byte{5, 6, 7, 8},
	}
	ctx := context.Background()

	tests := []struct {
		name        string
		setupMock   func(mock sqlmock.Sqlmock)
		dryRun      bool
		expectErr   bool
		errContains string
	}{
		{
			name: "Success",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(mockQ.SetVersionQuery)).
					WithArgs(v2.ID, hex.EncodeToString(v2.Checksum[:]), v1.ID, hex.EncodeToString(v1.Checksum[:])).
					WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectCommit()
			},
			dryRun:    false,
			expectErr: false,
		},
		{
			name: "SetVersion query error",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(mockQ.SetVersionQuery)).
					WithArgs(v2.ID, hex.EncodeToString(v2.Checksum[:]), v1.ID, hex.EncodeToString(v1.Checksum[:])).
					WillReturnError(errors.New("set version error"))
				mock.ExpectRollback()
			},
			dryRun:      false,
			expectErr:   true,
			errContains: "version error",
		},
		{
			name: "Bad versions error",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(mockQ.SetVersionQuery)).
					WithArgs(v2.ID, hex.EncodeToString(v2.Checksum[:]), v1.ID, hex.EncodeToString(v1.Checksum[:])).
					WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectRollback()
			},
			dryRun:      false,
			expectErr:   true,
			errContains: "bad version",
		},
		{
			name: "RowsAffected error",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(mockQ.SetVersionQuery)).
					WithArgs(v2.ID, hex.EncodeToString(v2.Checksum[:]), v1.ID, hex.EncodeToString(v1.Checksum[:])).
					WillReturnResult(&customResult{affectedErr: errors.New("affected error")})
				mock.ExpectRollback()
			},
			dryRun:      false,
			expectErr:   true,
			errContains: "affected error",
		}, {
			name: "Dry run",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(mockQ.SetVersionQuery)).
					WithArgs(v2.ID, hex.EncodeToString(v2.Checksum[:]), v1.ID, hex.EncodeToString(v1.Checksum[:])).
					WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectRollback()
			},
			dryRun:    true,
			expectErr: false,
		},
		{
			name: "Begin transaction error",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin().WillReturnError(errors.New("connection error"))
			},
			dryRun:      false,
			expectErr:   true,
			errContains: "connection error",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.setupMock(mock)

			err := db.DefaultStepFunc(ctx, &stepInfo{"name", v1, v2}, test.dryRun, NewNilLogger())

			if test.expectErr {
				if err == nil {
					t.Errorf("unexpected nil error")
				} else if test.errContains != "" && !strings.Contains(err.Error(), test.errContains) {
					t.Errorf("Expected error containing %q but got: %v", test.errContains, err)
				}
			} else if err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Expectations not met: %v", err)
			}
		})
	}
}

func TestTx(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()
	db := NewSQLDB(mockDB, mockQ)

	v1 := Version{
		ID:       100,
		Checksum: [32]byte{1, 2, 3, 4},
	}
	v2 := Version{
		ID:       123,
		Checksum: [32]byte{5, 6, 7, 8},
	}
	ctx := context.Background()

	query := `CREATE TABLE "test_table" ("id" INTEGER NOT NULL AUTOINCREMENT)`

	tests := []struct {
		name        string
		setupMock   func(mock sqlmock.Sqlmock)
		dryRun      bool
		expectErr   bool
		errContains string
	}{
		{
			name: "Success",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				rows := sqlmock.NewRows([]string{"id", "checksum"}).AddRow(v1.ID, hex.EncodeToString(v1.Checksum[:]))
				mock.ExpectQuery(regexp.QuoteMeta(mockQ.VersionQuery)).WillReturnRows(rows)
				mock.ExpectExec(regexp.QuoteMeta(query)).WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectExec(regexp.QuoteMeta(mockQ.SetVersionQuery)).
					WithArgs(v2.ID, hex.EncodeToString(v2.Checksum[:]), v1.ID, hex.EncodeToString(v1.Checksum[:])).
					WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectCommit()
			},
			dryRun:    false,
			expectErr: false,
		},
		{
			name: "Begin transaction error",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin().WillReturnError(errors.New("connection error"))
			},
			dryRun:      false,
			expectErr:   true,
			errContains: "connection error",
		},
		{
			name: "Dry run",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				rows := sqlmock.NewRows([]string{"id", "checksum"}).AddRow(v1.ID, hex.EncodeToString(v1.Checksum[:]))
				mock.ExpectQuery(regexp.QuoteMeta(mockQ.VersionQuery)).WillReturnRows(rows)
				mock.ExpectExec(regexp.QuoteMeta(query)).WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectExec(regexp.QuoteMeta(mockQ.SetVersionQuery)).
					WithArgs(v2.ID, hex.EncodeToString(v2.Checksum[:]), v1.ID, hex.EncodeToString(v1.Checksum[:])).
					WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectRollback()
			},
			dryRun:    true,
			expectErr: false,
		},
		{
			name: "Query version error",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectQuery(regexp.QuoteMeta(mockQ.VersionQuery)).WillReturnError(errors.New("query version error"))
				mock.ExpectRollback()
			},
			dryRun:      false,
			expectErr:   true,
			errContains: "query version error",
		},
		{
			name: "Version error",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				rows := sqlmock.NewRows([]string{"id", "checksum"}).AddRow(123, hex.EncodeToString(v1.Checksum[:]))
				mock.ExpectQuery(regexp.QuoteMeta(mockQ.VersionQuery)).WillReturnRows(rows)
				mock.ExpectRollback()
			},
			dryRun:      false,
			expectErr:   true,
			errContains: "db is v123",
		},
		{
			name: "Query error",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				rows := sqlmock.NewRows([]string{"id", "checksum"}).AddRow(v1.ID, hex.EncodeToString(v1.Checksum[:]))
				mock.ExpectQuery(regexp.QuoteMeta(mockQ.VersionQuery)).WillReturnRows(rows)
				mock.ExpectExec(regexp.QuoteMeta(query)).WillReturnError(errors.New("query error"))
				mock.ExpectRollback()
			},
			dryRun:      false,
			expectErr:   true,
			errContains: "query error",
		},
		{
			name: "Query error with dry run",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				rows := sqlmock.NewRows([]string{"id", "checksum"}).AddRow(v1.ID, hex.EncodeToString(v1.Checksum[:]))
				mock.ExpectQuery(regexp.QuoteMeta(mockQ.VersionQuery)).WillReturnRows(rows)
				mock.ExpectExec(regexp.QuoteMeta(query)).WillReturnError(errors.New("query error"))
				mock.ExpectRollback()
			},
			dryRun:      true,
			expectErr:   true,
			errContains: "dry run: query error",
		},
		{
			name: "Dry run and set version error",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				rows := sqlmock.NewRows([]string{"id", "checksum"}).AddRow(v1.ID, hex.EncodeToString(v1.Checksum[:]))
				mock.ExpectQuery(regexp.QuoteMeta(mockQ.VersionQuery)).WillReturnRows(rows)
				mock.ExpectExec(regexp.QuoteMeta(query)).WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectExec(regexp.QuoteMeta(mockQ.SetVersionQuery)).
					WithArgs(v2.ID, hex.EncodeToString(v2.Checksum[:]), v1.ID, hex.EncodeToString(v1.Checksum[:])).
					WillReturnError(errors.New("set version error"))
				mock.ExpectRollback()
			},
			dryRun:      true,
			expectErr:   true,
			errContains: "set version error",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.setupMock(mock)

			f := Tx(Cmd(query))
			err := f(ctx, db, &stepInfo{test.name, v1, v2}, test.dryRun, NewNilLogger())

			if test.expectErr {
				if err == nil {
					t.Errorf("unexpected nil error")
				} else if test.errContains != "" && !strings.Contains(err.Error(), test.errContains) {
					t.Errorf("Expected error containing %q but got: %v", test.errContains, err)
				}
			} else if err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Expectations not met: %v", err)
			}
		})
	}
}

func TestNotSQLDB(t *testing.T) {
	query := `CREATE TABLE "test_table" ("id" INTEGER NOT NULL AUTOINCREMENT)`
	v1 := Version{
		ID:       100,
		Checksum: [32]byte{1, 2, 3, 4},
	}
	v2 := Version{
		ID:       123,
		Checksum: [32]byte{5, 6, 7, 8},
	}
	ctx := context.Background()
	db := &mockDatabase{}

	f := Tx(Cmd(query))
	err := f(ctx, db, &stepInfo{"not SQLDB test", v1, v2}, false, NewNilLogger())
	if err == nil {
		t.Fatal("expect error")
	} else if !errors.Is(err, ErrNotSQLDB) {
		t.Fatalf("expect: %v, got %v", ErrNotSQLDB, err)
	}

	f = NoTx(Cmd(query))
	err = f(ctx, db, &stepInfo{"not SQLDB test", v1, v2}, false, NewNilLogger())
	if err == nil {
		t.Fatal("expect error")
	} else if !errors.Is(err, ErrNotSQLDB) {
		t.Fatalf("expect: %v, got %v", ErrNotSQLDB, err)
	}

	mockTxF := func(tx SQLTx, info StepInfo, dryRun bool, log Logger) error {
		return nil
	}

	f = TxF(mockTxF)
	err = f(ctx, db, &stepInfo{"not SQLDB test", v1, v2}, false, NewNilLogger())
	if err == nil {
		t.Fatal("expect error")
	} else if !errors.Is(err, ErrNotSQLDB) {
		t.Fatalf("expect: %v, got %v", ErrNotSQLDB, err)
	}

	mockNoTxF := func(ctx context.Context, db SQLDB, info StepInfo, log Logger) error {
		return nil
	}

	f = NoTxF(mockNoTxF)
	err = f(ctx, db, &stepInfo{"not SQLDB test", v1, v2}, false, NewNilLogger())
	if err == nil {
		t.Fatal("expect error")
	} else if !errors.Is(err, ErrNotSQLDB) {
		t.Fatalf("expect: %v, got %v", ErrNotSQLDB, err)
	}
}

func TestNoTx(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()
	db := NewSQLDB(mockDB, mockQ)

	v1 := Version{
		ID:       100,
		Checksum: [32]byte{1, 2, 3, 4},
	}
	v2 := Version{
		ID:       123,
		Checksum: [32]byte{5, 6, 7, 8},
	}
	ctx := context.Background()

	query := `CREATE TABLE "test_table" ("id" INTEGER NOT NULL AUTOINCREMENT)`

	tests := []struct {
		name        string
		setupMock   func(mock sqlmock.Sqlmock)
		dryRun      bool
		expectErr   bool
		errContains string
	}{
		{
			name: "Success",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"id", "checksum"}).AddRow(v1.ID, hex.EncodeToString(v1.Checksum[:]))
				mock.ExpectBegin()
				mock.ExpectQuery(regexp.QuoteMeta(mockQ.VersionQuery)).WillReturnRows(rows)
				mock.ExpectCommit()
				mock.ExpectExec(regexp.QuoteMeta(query)).WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(mockQ.SetVersionQuery)).
					WithArgs(v2.ID, hex.EncodeToString(v2.Checksum[:]), v1.ID, hex.EncodeToString(v1.Checksum[:])).
					WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectCommit()
			},
			dryRun:    false,
			expectErr: false,
		},
		{
			name:      "Dry run",
			setupMock: func(mock sqlmock.Sqlmock) {},
			dryRun:    true,
			expectErr: false,
		},
		{
			name: "Query version error",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectQuery(regexp.QuoteMeta(mockQ.VersionQuery)).WillReturnError(errors.New("query version error"))
				mock.ExpectRollback()
			},
			dryRun:      false,
			expectErr:   true,
			errContains: "query version error",
		},
		{
			name: "Version error",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				rows := sqlmock.NewRows([]string{"id", "checksum"}).AddRow(123, hex.EncodeToString(v1.Checksum[:]))
				mock.ExpectQuery(regexp.QuoteMeta(mockQ.VersionQuery)).WillReturnRows(rows)
				mock.ExpectCommit()
			},
			dryRun:      false,
			expectErr:   true,
			errContains: "db is v123",
		},
		{
			name: "Query error",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				rows := sqlmock.NewRows([]string{"id", "checksum"}).AddRow(v1.ID, hex.EncodeToString(v1.Checksum[:]))
				mock.ExpectQuery(regexp.QuoteMeta(mockQ.VersionQuery)).WillReturnRows(rows)
				mock.ExpectCommit()
				mock.ExpectExec(regexp.QuoteMeta(query)).WillReturnError(errors.New("query error"))
			},
			dryRun:      false,
			expectErr:   true,
			errContains: "query error",
		},
		{
			name:      "Query error with dry run",
			setupMock: func(mock sqlmock.Sqlmock) {},
			dryRun:    true,
			expectErr: false,
		},
		{
			name: "Set version error",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				rows := sqlmock.NewRows([]string{"id", "checksum"}).AddRow(v1.ID, hex.EncodeToString(v1.Checksum[:]))
				mock.ExpectQuery(regexp.QuoteMeta(mockQ.VersionQuery)).WillReturnRows(rows)
				mock.ExpectCommit()
				mock.ExpectExec(regexp.QuoteMeta(query)).WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(mockQ.SetVersionQuery)).
					WithArgs(v2.ID, hex.EncodeToString(v2.Checksum[:]), v1.ID, hex.EncodeToString(v1.Checksum[:])).
					WillReturnError(errors.New("set version error"))
				mock.ExpectRollback()
			},
			dryRun:      false,
			expectErr:   true,
			errContains: "set version error",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.setupMock(mock)

			f := NoTx(Cmd(query))
			err := f(ctx, db, &stepInfo{test.name, v1, v2}, test.dryRun, NewNilLogger())

			if test.expectErr {
				if err == nil {
					t.Errorf("unexpected nil error")
				} else if test.errContains != "" && !strings.Contains(err.Error(), test.errContains) {
					t.Errorf("Expected error containing %q but got: %v", test.errContains, err)
				}
			} else if err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Expectations not met: %v", err)
			}
		})
	}
}

func TestTxF(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()
	db := NewSQLDB(mockDB, mockQ)

	v1 := Version{
		ID:       100,
		Checksum: [32]byte{1, 2, 3, 4},
	}
	v2 := Version{
		ID:       123,
		Checksum: [32]byte{5, 6, 7, 8},
	}
	ctx := context.Background()

	var funcError error
	var funcExecuted bool
	funcExecutedPtr := &funcExecuted
	mockTxF := func(tx SQLTx, info StepInfo, dryRun bool, log Logger) error {
		*funcExecutedPtr = true
		return funcError
	}
	tests := []struct {
		name         string
		setupMock    func(mock sqlmock.Sqlmock)
		dryRun       bool
		expectErr    bool
		errContains  string
		funcExecuted bool
		funcError    error
	}{
		{
			name: "Success",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				rows := sqlmock.NewRows([]string{"id", "checksum"}).AddRow(v1.ID, hex.EncodeToString(v1.Checksum[:]))
				mock.ExpectQuery(regexp.QuoteMeta(mockQ.VersionQuery)).WillReturnRows(rows)
				mock.ExpectExec(regexp.QuoteMeta(mockQ.SetVersionQuery)).
					WithArgs(v2.ID, hex.EncodeToString(v2.Checksum[:]), v1.ID, hex.EncodeToString(v1.Checksum[:])).
					WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectCommit()
			},
			dryRun:       false,
			expectErr:    false,
			funcExecuted: true,
		},
		{
			name: "Begin transaction error",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin().WillReturnError(errors.New("connection error"))
			},
			dryRun:      false,
			expectErr:   true,
			errContains: "connection error",
		},
		{
			name: "Dry run",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				rows := sqlmock.NewRows([]string{"id", "checksum"}).AddRow(v1.ID, hex.EncodeToString(v1.Checksum[:]))
				mock.ExpectQuery(regexp.QuoteMeta(mockQ.VersionQuery)).WillReturnRows(rows)
				mock.ExpectExec(regexp.QuoteMeta(mockQ.SetVersionQuery)).
					WithArgs(v2.ID, hex.EncodeToString(v2.Checksum[:]), v1.ID, hex.EncodeToString(v1.Checksum[:])).
					WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectRollback()
			},
			dryRun:       true,
			expectErr:    false,
			funcExecuted: true,
		},
		{
			name: "Query version error",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectQuery(regexp.QuoteMeta(mockQ.VersionQuery)).WillReturnError(errors.New("query version error"))
				mock.ExpectRollback()
			},
			dryRun:      false,
			expectErr:   true,
			errContains: "query version error",
		},
		{
			name: "Version error",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				rows := sqlmock.NewRows([]string{"id", "checksum"}).AddRow(123, hex.EncodeToString(v1.Checksum[:]))
				mock.ExpectQuery(regexp.QuoteMeta(mockQ.VersionQuery)).WillReturnRows(rows)
				mock.ExpectRollback()
			},
			dryRun:      false,
			expectErr:   true,
			errContains: "db is v123",
		},
		{
			name: "Dry run and set version error",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				rows := sqlmock.NewRows([]string{"id", "checksum"}).AddRow(v1.ID, hex.EncodeToString(v1.Checksum[:]))
				mock.ExpectQuery(regexp.QuoteMeta(mockQ.VersionQuery)).WillReturnRows(rows)
				mock.ExpectExec(regexp.QuoteMeta(mockQ.SetVersionQuery)).
					WithArgs(v2.ID, hex.EncodeToString(v2.Checksum[:]), v1.ID, hex.EncodeToString(v1.Checksum[:])).
					WillReturnError(errors.New("set version error"))
				mock.ExpectRollback()
			},
			dryRun:       true,
			expectErr:    true,
			errContains:  "set version error",
			funcExecuted: true,
		},
		{
			name: "function abort error",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				rows := sqlmock.NewRows([]string{"id", "checksum"}).AddRow(v1.ID, hex.EncodeToString(v1.Checksum[:]))
				mock.ExpectQuery(regexp.QuoteMeta(mockQ.VersionQuery)).WillReturnRows(rows)
				mock.ExpectRollback()
			},
			dryRun:       false,
			expectErr:    true,
			errContains:  "abort transaction",
			funcExecuted: true,
			funcError:    ErrAbort,
		},
		{
			name: "function cancel error",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				rows := sqlmock.NewRows([]string{"id", "checksum"}).AddRow(v1.ID, hex.EncodeToString(v1.Checksum[:]))
				mock.ExpectQuery(regexp.QuoteMeta(mockQ.VersionQuery)).WillReturnRows(rows)
				mock.ExpectRollback()
			},
			dryRun:       false,
			expectErr:    false,
			funcExecuted: true,
			funcError:    ErrCancel,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.setupMock(mock)

			funcError = test.funcError
			funcExecuted = false
			f := TxF(mockTxF)
			err := f(ctx, db, &stepInfo{test.name, v1, v2}, test.dryRun, NewNilLogger())
			if test.funcExecuted != funcExecuted {
				t.Fatalf("expect function executed %v, got %v", test.funcExecuted, funcExecuted)
			}
			if test.expectErr {
				if err == nil {
					t.Errorf("unexpected nil error")
				} else if test.errContains != "" && !strings.Contains(err.Error(), test.errContains) {
					t.Errorf("Expected error containing %q but got: %v", test.errContains, err)
				}
			} else if err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Expectations not met: %v", err)
			}
		})
	}
}

func TestNoTxF(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()
	db := NewSQLDB(mockDB, mockQ)

	v1 := Version{
		ID:       100,
		Checksum: [32]byte{1, 2, 3, 4},
	}
	v2 := Version{
		ID:       123,
		Checksum: [32]byte{5, 6, 7, 8},
	}
	ctx := context.Background()

	var funcError error
	var funcExecuted bool
	funcExecutedPtr := &funcExecuted

	mockNoTxF := func(ctx context.Context, db SQLDB, info StepInfo, log Logger) error {
		*funcExecutedPtr = true
		return funcError
	}
	tests := []struct {
		name         string
		setupMock    func(mock sqlmock.Sqlmock)
		dryRun       bool
		expectErr    bool
		errContains  string
		funcExecuted bool
		funcError    error
	}{
		{
			name: "Success",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"id", "checksum"}).AddRow(v1.ID, hex.EncodeToString(v1.Checksum[:]))
				mock.ExpectBegin()
				mock.ExpectQuery(regexp.QuoteMeta(mockQ.VersionQuery)).WillReturnRows(rows)
				mock.ExpectCommit()
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(mockQ.SetVersionQuery)).
					WithArgs(v2.ID, hex.EncodeToString(v2.Checksum[:]), v1.ID, hex.EncodeToString(v1.Checksum[:])).
					WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectCommit()
			},
			dryRun:       false,
			expectErr:    false,
			funcExecuted: true,
		},
		{
			name:         "Dry run",
			setupMock:    func(mock sqlmock.Sqlmock) {},
			dryRun:       true,
			expectErr:    false,
			funcExecuted: false,
		},
		{
			name: "Query version error",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectQuery(regexp.QuoteMeta(mockQ.VersionQuery)).WillReturnError(errors.New("query version error"))
				mock.ExpectRollback()
			},
			dryRun:      false,
			expectErr:   true,
			errContains: "query version error",
		},
		{
			name: "Version error",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"id", "checksum"}).AddRow(123, hex.EncodeToString(v1.Checksum[:]))
				mock.ExpectBegin()
				mock.ExpectQuery(regexp.QuoteMeta(mockQ.VersionQuery)).WillReturnRows(rows)
				mock.ExpectCommit()
			},
			dryRun:      false,
			expectErr:   true,
			errContains: "db is v123",
		},
		{
			name: "Begin transaction error",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"id", "checksum"}).AddRow(v1.ID, hex.EncodeToString(v1.Checksum[:]))
				mock.ExpectBegin()
				mock.ExpectQuery(regexp.QuoteMeta(mockQ.VersionQuery)).WillReturnRows(rows)
				mock.ExpectCommit()
				mock.ExpectBegin().WillReturnError(errors.New("connection error"))
			},
			dryRun:       false,
			expectErr:    true,
			errContains:  "connection error",
			funcExecuted: true,
		},
		{
			name: "Set version error",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"id", "checksum"}).AddRow(v1.ID, hex.EncodeToString(v1.Checksum[:]))
				mock.ExpectBegin()
				mock.ExpectQuery(regexp.QuoteMeta(mockQ.VersionQuery)).WillReturnRows(rows)
				mock.ExpectCommit()
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(mockQ.SetVersionQuery)).
					WithArgs(v2.ID, hex.EncodeToString(v2.Checksum[:]), v1.ID, hex.EncodeToString(v1.Checksum[:])).
					WillReturnError(errors.New("set version error"))
				mock.ExpectRollback()
			},
			dryRun:       false,
			expectErr:    true,
			errContains:  "set version error",
			funcExecuted: true,
		},
		{
			name: "function abort error",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"id", "checksum"}).AddRow(v1.ID, hex.EncodeToString(v1.Checksum[:]))
				mock.ExpectBegin()
				mock.ExpectQuery(regexp.QuoteMeta(mockQ.VersionQuery)).WillReturnRows(rows)
				mock.ExpectCommit()
			},
			dryRun:       false,
			expectErr:    true,
			errContains:  "abort transaction",
			funcExecuted: true,
			funcError:    ErrAbort,
		},
		{
			name: "function cancel error",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"id", "checksum"}).AddRow(v1.ID, hex.EncodeToString(v1.Checksum[:]))
				mock.ExpectBegin()
				mock.ExpectQuery(regexp.QuoteMeta(mockQ.VersionQuery)).WillReturnRows(rows)
				mock.ExpectCommit()
			},
			dryRun:       false,
			expectErr:    true,
			errContains:  "cancel transaction",
			funcExecuted: true,
			funcError:    ErrCancel,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.setupMock(mock)

			funcError = test.funcError
			funcExecuted = false
			f := NoTxF(mockNoTxF)
			err := f(ctx, db, &stepInfo{test.name, v1, v2}, test.dryRun, NewNilLogger())
			if test.funcExecuted != funcExecuted {
				t.Fatalf("expect function executed %v, got %v", test.funcExecuted, funcExecuted)
			}
			if test.expectErr {
				if err == nil {
					t.Errorf("unexpected nil error")
				} else if test.errContains != "" && !strings.Contains(err.Error(), test.errContains) {
					t.Errorf("Expected error containing %q but got: %v", test.errContains, err)
				}
			} else if err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Expectations not met: %v", err)
			}
		})
	}
}
