package migrate

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
)

// TestNewSteps tests the creation of a new Steps instance
func TestNewSteps(t *testing.T) {
	name := "test-db"
	steps := NewSteps(name)

	if steps == nil {
		t.Fatal("NewSteps() returned nil")
	}

	if len(steps.steps) != 1 {
		t.Errorf("Expected 1 initial step, got %d", len(steps.steps))
	}

	// Check that the first step has the correct name
	if steps.steps[0].name != name {
		t.Errorf("First step name = %v, want %v", steps.steps[0].name, name)
	}

	// Check that the first step has the correct version
	expectedChecksum := sha256.Sum256([]byte(name))
	if steps.steps[0].version.Checksum != expectedChecksum {
		t.Errorf("First step checksum = %v, want %v", steps.steps[0].version.Checksum, expectedChecksum)
	}
}

// TestSteps_Append tests appending steps to a Steps instance
func TestSteps_Append(t *testing.T) {
	// Setup
	steps := NewSteps("test-db")

	// Test appending with empty name
	err := steps.Append("", nil, nil)
	if err == nil {
		t.Error("Expected error for empty name, got nil")
	}

	// Test successful append
	testUpFunc := func(context.Context, Database, StepInfo, bool, Logger) error { return nil }
	testDownFunc := func(context.Context, Database, StepInfo, bool, Logger) error { return nil }

	err = steps.Append("step1", testUpFunc, testDownFunc)
	if err != nil {
		t.Errorf("Unexpected error on append: %v", err)
	}

	if len(steps.steps) != 2 {
		t.Errorf("Expected 2 steps after append, got %d", len(steps.steps))
	}

	// Test that the step properties are correctly set
	step := steps.steps[1]
	if step.name != "step1" {
		t.Errorf("Appended step name = %v, want %v", step.name, "step1")
	}

	// Test checksum generation logic
	var b []byte
	b = append(b, steps.steps[0].version.Checksum[:]...)
	b = binary.LittleEndian.AppendUint64(b, uint64(1))
	b = append(b, "step1"...)
	expectedChecksum := sha256.Sum256(b)

	if step.version.Checksum != expectedChecksum {
		t.Errorf("Appended step checksum = %v, want %v", step.version.Checksum, expectedChecksum)
	}

	// Verify that the functions are correctly assigned
	if fmt.Sprintf("%p", step.up) != fmt.Sprintf("%p", testUpFunc) {
		t.Error("Up function was not correctly assigned")
	}

	if fmt.Sprintf("%p", step.down) != fmt.Sprintf("%p", testDownFunc) {
		t.Error("Down function was not correctly assigned")
	}
}

// TestSteps_Len tests the Len method of Steps
func TestSteps_Len(t *testing.T) {
	// Setup
	steps := NewSteps("test-db")

	// Test initial length
	if steps.Len() != 1 {
		t.Errorf("Initial Len() = %d, want 1", steps.Len())
	}

	// Add a step
	_ = steps.Append("step1", nil, nil)

	// Test length after append
	if steps.Len() != 2 {
		t.Errorf("Len() after append = %d, want 2", steps.Len())
	}
}

// TestSteps_Version tests the Version method of Steps
func TestSteps_Version(t *testing.T) {
	// Setup
	steps := NewSteps("test-db")
	_ = steps.Append("step1", nil, nil)

	// Test valid ID
	v, err := steps.Version(1)
	if err != nil {
		t.Errorf("Unexpected error for valid ID: %v", err)
	}
	if v.ID != 1 {
		t.Errorf("Version ID = %d, want 1", v.ID)
	}

	// Test invalid ID (negative)
	_, err = steps.Version(-1)
	if err == nil {
		t.Error("Expected error for negative ID, got nil")
	}
	if !errors.Is(err, ErrBadVersionID) {
		t.Errorf("Expected ErrBadVersionID, got %v", err)
	}

	// Test invalid ID (too large)
	_, err = steps.Version(100)
	if err == nil {
		t.Error("Expected error for too large ID, got nil")
	}
	if !errors.Is(err, ErrBadVersionID) {
		t.Errorf("Expected ErrBadVersionID, got %v", err)
	}
}

// TestSteps_Check tests the Check method of Steps
func TestSteps_Check(t *testing.T) {
	// Setup
	steps := NewSteps("test-db")
	_ = steps.Append("step1", nil, nil)

	// Get a valid version
	validVersion, _ := steps.Version(1)

	// Test valid version
	err := steps.Check(validVersion)
	if err != nil {
		t.Errorf("Unexpected error for valid version: %v", err)
	}

	// Test invalid ID
	invalidIDVersion := Version{ID: 100}
	err = steps.Check(invalidIDVersion)
	if err == nil {
		t.Error("Expected error for invalid ID, got nil")
	}
	if !errors.Is(err, ErrBadVersionID) {
		t.Errorf("Expected ErrBadVersionID, got %v", err)
	}

	// Test invalid checksum
	invalidChecksumVersion := validVersion
	invalidChecksumVersion.Checksum[0] = invalidChecksumVersion.Checksum[0] + 1 // Modify the checksum
	err = steps.Check(invalidChecksumVersion)
	if err == nil {
		t.Error("Expected error for invalid checksum, got nil")
	}
	if !errors.Is(err, ErrBadVersionChecksum) {
		t.Errorf("Expected ErrBadVersionChecksum, got %v", err)
	}
}

// TestSteps_Up tests the Up method of Steps
func TestSteps_Up(t *testing.T) {
	// Setup
	steps := NewSteps("test-db")
	testUpFunc := func(context.Context, Database, StepInfo, bool, Logger) error { return nil }
	_ = steps.Append("step1", testUpFunc, nil)

	// Get a valid version
	initialVersion, _ := steps.Version(0)

	name, _ := steps.Name(0)
	if exp := "test-db"; name != exp {
		t.Fatalf("expect %q, got %q", exp, name)
	}
	name, err := steps.Name(10)
	if name != "" {
		t.Fatalf("expect %q, got %q", "", name)
	}
	if err == nil {
		t.Fatal("expect error")
	} else if !errors.Is(err, ErrBadVersionID) {
		t.Fatal(err)
	}

	// Test valid up migration
	info, fn, err := steps.Up(initialVersion)
	if err != nil {
		t.Errorf("Unexpected error for valid up migration: %v", err)
	}
	if info.Name() != "step1" {
		t.Errorf("Step name = %v, want %v", info.Name(), "step1")
	}
	if info.From() != initialVersion {
		t.Errorf("From version = %v, want %v", info.From(), initialVersion)
	}
	if fmt.Sprintf("%p", fn) != fmt.Sprintf("%p", testUpFunc) {
		t.Error("Returned function does not match expected up function")
	}
	if exp := "'step1' v0:0f3182d0b4... -> v1:b4d9cdaf09..."; exp != info.String() {
		t.Errorf("expect %q, got %q", exp, info.String())
	}

	// Test with invalid version
	invalidVersion := Version{ID: 100}
	_, _, err = steps.Up(invalidVersion)
	if err == nil {
		t.Error("Expected error for invalid version, got nil")
	}
	if !errors.Is(err, ErrBadVersion) {
		t.Errorf("Expected ErrBadVersion, got %v", err)
	}

	// Test at end of steps
	lastVersion, _ := steps.Version(1)
	_, _, err = steps.Up(lastVersion)
	if err == nil {
		t.Error("Expected error at end of steps, got nil")
	}
	if !errors.Is(err, ErrEndOfSteps) {
		t.Errorf("Expected ErrEndOfSteps, got %v", err)
	}
}

// TestSteps_Down tests the Down method of Steps
func TestSteps_Down(t *testing.T) {
	// Setup
	steps := NewSteps("test-db")
	testDownFunc := func(context.Context, Database, StepInfo, bool, Logger) error { return nil }
	_ = steps.Append("step1", nil, testDownFunc)

	// Get a valid version
	initialVersion, err := steps.Version(1)
	if err != nil {
		t.Fatal(err)
	}

	// Test valid down migration
	info, fn, err := steps.Down(initialVersion)
	if err != nil {
		t.Errorf("Unexpected error for valid down migration: %v", err)
	}
	if info.Name() != "step1" {
		t.Errorf("Step name = %v, want %v", info.Name(), "step1")
	}
	if info.From() != initialVersion {
		t.Errorf("From version = %v, want %v", info.From(), initialVersion)
	}
	if fmt.Sprintf("%p", fn) != fmt.Sprintf("%p", testDownFunc) {
		t.Error("Returned function does not match expected down function")
	}

	// Test with invalid version
	invalidVersion := Version{ID: 100}
	_, _, err = steps.Down(invalidVersion)
	if err == nil {
		t.Error("Expected error for invalid version, got nil")
	}
	if !errors.Is(err, ErrBadVersion) {
		t.Errorf("Expected ErrBadVersion, got %v", err)
	}

	// Test at end of steps
	lastVersion, _ := steps.Version(0)
	_, _, err = steps.Down(lastVersion)
	if err == nil {
		t.Error("Expected error at end of steps, got nil")
	}
	if !errors.Is(err, ErrEndOfSteps) {
		t.Errorf("Expected ErrEndOfSteps, got %v", err)
	}
}

// TestSteps_Concurrency tests that the Steps methods can be safely called concurrently
func TestSteps_Concurrency(t *testing.T) {
	// Create a Steps instance with a few steps
	steps := NewSteps("test-db")
	_ = steps.Append("step1", nil, nil)
	_ = steps.Append("step2", nil, nil)

	// Run a bunch of goroutines that access the Steps methods concurrently
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func() {
			// Call various methods
			_ = steps.Len()
			_, _ = steps.Version(0)
			_ = steps.Check(Version{ID: 0})
			_, _, _ = steps.Up(Version{ID: 0})
			_, _, _ = steps.Down(Version{ID: 0})

			done <- true
		}()
	}

	// Wait for all goroutines to finish
	for i := 0; i < 10; i++ {
		<-done
	}

	// If we get here without a deadlock or panic, the test passes
}
