package migrate

import "testing"

func TestError(t *testing.T) {
	if ErrCancel.Error() != "cancel transaction" {
		t.Fatal("expect cancel transaction")
	}
}
