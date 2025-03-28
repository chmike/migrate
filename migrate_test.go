package migrate

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// Mock types

type mockDatabase struct {
	version       Version
	initialized   bool
	initError     error
	setVersionErr error
	versionErr    error
}

func (m *mockDatabase) InitVersion(ctx context.Context, v Version, dryRun bool) error {
	if m.initialized {
		return ErrAlreadyInitialized
	}
	if m.initError != nil {
		return m.initError
	}
	if !dryRun {
		m.version = v
		m.initialized = true
	}
	return nil
}

func (m *mockDatabase) Version(ctx context.Context) (Version, error) {
	if m.versionErr != nil {
		return badVersion, m.versionErr
	}
	return m.version, nil
}

func (m *mockDatabase) DefaultStepFunc(ctx context.Context, info StepInfo, dryRun bool, log Logger) error {
	if m.setVersionErr != nil {
		return m.setVersionErr
	}
	if !dryRun {
		m.version = info.To()
	}
	return nil
}

type mockStepper struct {
	steps []StepFunc
}

func (m *mockStepper) Len() int {
	return len(m.steps)
}

func (m *mockStepper) Version(ID int) (Version, error) {
	if ID < 0 || ID >= len(m.steps) {
		return badVersion, ErrBadVersion
	}
	return Version{ID: ID}, nil
}

func (m *mockStepper) Name(ID int) (string, error) {
	if ID < 0 || ID >= len(m.steps) {
		return "", ErrBadVersion
	}
	return fmt.Sprintf("step %d", ID), nil
}

func (m *mockStepper) Check(v Version) error {
	if v.ID < 0 || v.ID >= len(m.steps) {
		return ErrBadVersion
	}
	return nil
}

func (m *mockStepper) Up(v Version) (StepInfo, StepFunc, error) {
	if err := m.Check(v); err != nil {
		return nil, nil, err
	}
	nv, err := m.Version(v.ID + 1)
	if err != nil {
		return nil, nil, ErrEndOfSteps
	}
	return &stepInfo{name: fmt.Sprintf("step %d", nv.ID), from: v, to: nv}, m.steps[nv.ID], err
}

func (m *mockStepper) Down(v Version) (StepInfo, StepFunc, error) {
	if err := m.Check(v); err != nil {
		return nil, nil, err
	}
	nv, err := m.Version(v.ID - 1)
	if err != nil {
		return nil, nil, ErrEndOfSteps
	}
	return &stepInfo{name: fmt.Sprintf("step %d", v.ID), from: v, to: nv}, m.steps[v.ID], err
}

// Unit tests
var mockFunc = func(ctx context.Context, db Database, info StepInfo, dryRun bool, log Logger) error {
	return db.DefaultStepFunc(ctx, info, dryRun, log)
}

var errMock = errors.New("mock error")

func TestNew(t *testing.T) {
	m, err := New(nil, nil, nil)
	if err == nil {
		t.Fatal("expect error")
	}
	if m != nil {
		t.Fatal("expect nil migrator")
	}

	db := &mockDatabase{version: Version{ID: 2}}
	steps := &mockStepper{[]StepFunc{nil, mockFunc, mockFunc}}
	m, err = New(db, steps, nil)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if m == nil {
		t.Fatal("expect non-nil migrator")
	}
}

func TestMigratorVersion(t *testing.T) {
	db := &mockDatabase{version: Version{ID: 2}}
	steps := &mockStepper{[]StepFunc{nil, mockFunc, mockFunc}}
	m, err := New(db, steps, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	v, err := m.Version()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v.ID != 2 {
		t.Fatalf("expected version 2, got %v ", v)
	}

	db.versionErr = errMock
	v, err = m.Version()
	if err == nil {
		t.Fatal("expect error")
	} else {
		if !errors.Is(err, errMock) {
			t.Fatalf("expect %q, got %q", errMock, err)
		}
	}
	db.versionErr = nil

	db.version.ID = 10
	v, err = m.Version()
	if err == nil {
		t.Fatal("expect error")
	} else if !errors.Is(err, ErrBadVersion) {
		t.Fatalf("expect %q, got %q", ErrBadVersion, err)
	}

}

func TestMigratorInit(t *testing.T) {
	db := &mockDatabase{versionErr: errMock}
	steps := &mockStepper{[]StepFunc{nil, mockFunc, mockFunc}}

	m, err := New(db, steps, nil)
	if err != nil {
		t.Fatal("unexpected error:", err)
	}

	err = m.Init()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if db.version.ID != 0 {
		t.Fatalf("expect version 0, got %d", db.version.ID)
	}

	db.versionErr = nil
	err = m.Init()
	if err == nil {
		t.Fatalf("expect error")
	} else if !errors.Is(err, ErrAlreadyInitialized) {
		t.Fatalf("expect %q, got %q", ErrAlreadyInitialized, err)
	}

	db.initError = errMock
	db.version = badVersion
	err = m.Init()
	if err == nil {
		t.Fatalf("expect error")
	} else if !errors.Is(err, ErrNotInitialized) {
		t.Fatalf("expect %q, got %q", ErrNotInitialized, err)
	}
	db.initError = nil

	db.versionErr = errMock
	db.initialized = false
	err = m.Init()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if db.version.ID != 0 {
		t.Fatalf("expect version 0, got %d", db.version.ID)
	}
	if !db.initialized {
		t.Fatal("expect initialized")
	}

	db.versionErr = errMock
	db.initialized = false
	err = m.InitDryRun()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if db.version.ID != 0 {
		t.Fatalf("expect version 0, got %d", db.version.ID)
	}
	if db.initialized {
		t.Fatal("expect not initialized")
	}

	db.versionErr = errMock
	db.initialized = false
	steps.steps = steps.steps[:0]
	err = m.Init()
	if err == nil {
		t.Fatalf("expect error")
	} else if !errors.Is(err, ErrNotInitialized) {
		t.Fatalf("expect %q, got %q", ErrNotInitialized, err)
	}

}

func TestMigrator(t *testing.T) {
	tests := []struct {
		name   string
		op     func(m *Migrator) error
		err    error
		v      Version
		expErr error
		expV   Version
	}{
		{
			name: "test OneUp",
			op: func(m *Migrator) error {
				return m.OneUp()
			},
			v:    Version{ID: 1},
			expV: Version{ID: 2},
		},
		{
			name: "test OneUp error",
			op: func(m *Migrator) error {
				return m.OneUp()
			},
			v:      Version{ID: 1},
			expV:   Version{ID: 1},
			err:    errMock,
			expErr: errMock,
		},
		{
			name: "test OneUpDryRun",
			op: func(m *Migrator) error {
				return m.OneUpDryRun()
			},
			v:    Version{ID: 1},
			expV: Version{ID: 1},
		},
		{
			name: "test OneUpDryRun error",
			op: func(m *Migrator) error {
				return m.OneUpDryRun()
			},
			v:      Version{ID: 1},
			expV:   Version{ID: 1},
			err:    errMock,
			expErr: errMock,
		},
		{
			name: "test OneDown",
			op: func(m *Migrator) error {
				return m.OneDown()
			},
			v:    Version{ID: 1},
			expV: Version{ID: 0},
		},
		{
			name: "test OneDown error",
			op: func(m *Migrator) error {
				return m.OneDown()
			},
			v:      Version{ID: 1},
			expV:   Version{ID: 1},
			err:    errMock,
			expErr: errMock,
		},
		{
			name: "test OneDownDryRun",
			op: func(m *Migrator) error {
				return m.OneDownDryRun()
			},
			v:    Version{ID: 1},
			expV: Version{ID: 1},
		},
		{
			name: "test OneDownDryRun error",
			op: func(m *Migrator) error {
				return m.OneDownDryRun()
			},
			v:      Version{ID: 1},
			expV:   Version{ID: 1},
			err:    errMock,
			expErr: errMock,
		},
		{
			name: "test AllUp",
			op: func(m *Migrator) error {
				return m.AllUp()
			},
			v:    Version{ID: 0},
			expV: Version{ID: 2},
		},
		{
			name: "test AllUp error",
			op: func(m *Migrator) error {
				return m.AllUp()
			},
			v:      Version{ID: 0},
			expV:   Version{ID: 0},
			err:    errMock,
			expErr: errMock,
		},
		{
			name: "test AllDown",
			op: func(m *Migrator) error {
				return m.AllDown()
			},
			v:    Version{ID: 2},
			expV: Version{ID: 0},
		},
		{
			name: "test AllDown error",
			op: func(m *Migrator) error {
				return m.AllDown()
			},
			v:      Version{ID: 2},
			expV:   Version{ID: 2},
			err:    errMock,
			expErr: errMock,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := &mockDatabase{version: test.v, setVersionErr: test.err}
			steps := &mockStepper{[]StepFunc{nil, mockFunc, nil}}
			m, _ := New(db, steps, nil)
			v, err := m.Version()
			if err != nil {
				t.Fatal(err)
			}
			if v != db.version {
				t.Fatalf("expect %v, got %v", db.version, v)
			}
			err = test.op(m)
			if test.expErr == nil {
				if err != nil {
					t.Errorf("expect nil error, got %v", err)
				}
			} else if err == nil {
				t.Fatal("expect error")
			} else if !errors.Is(err, test.expErr) {
				t.Fatalf("expect error %q, got %q", test.expErr, err)
			} else if db.version != test.expV {
				t.Fatalf("expect %v, got %v", test.expV, db.version)
			}
		})
	}
}
