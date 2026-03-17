package datastruct

import "testing"

func TestBytesString_SerializeRoundTrip(t *testing.T) {
	dv := &DataValue{
		Value:          &BytesString{Data: []byte("value")},
		ExpireTime:     0,
		LastAccessedAt: 123,
	}
	b, err := dv.Serialize()
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}

	out, err := DeserializeDataValue(b)
	if err != nil {
		t.Fatalf("deserialize: %v", err)
	}

	bs, ok := out.Value.(*BytesString)
	if !ok {
		t.Fatalf("unexpected type: %T", out.Value)
	}
	if string(bs.Data) != "value" {
		t.Fatalf("unexpected data: %q", string(bs.Data))
	}
}

