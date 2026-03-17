package database

import "testing"

func TestKeyTopK_Top(t *testing.T) {
	tk := NewKeyTopK(3)
	for i := 0; i < 10; i++ {
		tk.Add("a")
	}
	for i := 0; i < 7; i++ {
		tk.Add("b")
	}
	for i := 0; i < 3; i++ {
		tk.Add("c")
	}
	for i := 0; i < 2; i++ {
		tk.Add("d")
	}

	top := tk.Top(3)
	if len(top) != 3 {
		t.Fatalf("expected 3, got %d", len(top))
	}
	if top[0].Key != "a" {
		t.Fatalf("expected a, got %s", top[0].Key)
	}
	if top[1].Key != "b" {
		t.Fatalf("expected b, got %s", top[1].Key)
	}
}

func TestKeyTopK_ZeroK(t *testing.T) {
	tk := NewKeyTopK(0)
	tk.Add("a")
	if got := tk.Top(10); len(got) != 0 {
		t.Fatalf("expected empty, got %d", len(got))
	}
}
