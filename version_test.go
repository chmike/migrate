package migrate

import (
	"errors"
	"testing"
)

func TestVersion(t *testing.T) {
	if exp := "v-1:0000000000..."; BadVersion.String() != exp {
		t.Fatalf("expect %q, got %q", exp, BadVersion)
	}

	out, err := MakeVersion(2, "00010203040506070809010B0C0D0E0F0010203040506070809010B0C0D0E0F0")
	if err != nil {
		t.Fatal(err)
	}

	exp := Version{
		ID: 2,
		Checksum: [32]byte{
			0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x01, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F,
			0x00, 0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80, 0x90, 0x10, 0xB0, 0xC0, 0xD0, 0xE0, 0xF0,
		},
	}
	if exp != out {
		t.Fatalf("expect %q, got %q", exp, out)
	}

	out, err = MakeVersion(-1, "00010203040506070809010B0C0D0E0F0010203040506070809010B0C0D0E0F0")
	if err == nil {
		t.Fatal("expect error")
	} else if !errors.Is(err, ErrBadVersionID) {
		t.Fatal("expect bad version id error")
	}
	if out != badVersion {
		t.Fatalf("expect %v, got %v", badVersion, out)
	}

	out, err = MakeVersion(2, "Z0010203040506070809010B0C0D0E0F0010203040506070809010B0C0D0E0F0")
	if err == nil {
		t.Fatal("expect error")
	} else if !errors.Is(err, ErrBadVersionChecksum) {
		t.Fatal("expect bad version id error")
	}
	if out != badVersion {
		t.Fatalf("expect %v, got %v", badVersion, out)
	}

	out, err = MakeVersion(2, "000102030405060")
	if err == nil {
		t.Fatal("expect error")
	} else if !errors.Is(err, ErrBadVersionChecksum) {
		t.Fatal("expect bad version id error")
	}
	if out != badVersion {
		t.Fatalf("expect %v, got %v", badVersion, out)
	}

	out, err = MakeVersion(2, "00010203040506")
	if err == nil {
		t.Fatal("expect error")
	} else if !errors.Is(err, ErrBadVersionChecksum) {
		t.Fatal("expect bad version id error")
	}
	if out != badVersion {
		t.Fatalf("expect %v, got %v", badVersion, out)
	}
}
