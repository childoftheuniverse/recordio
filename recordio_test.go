package recordio

import (
	"github.com/childoftheuniverse/filesystem-internal"
	"golang.org/x/net/context"
	"testing"
)

/*
Write two records to a temporary buffer, then read them back as records.
Checks that the records have the requested length.
*/
func TestSerializeAndReadBytes(t *testing.T) {
	var ctx = context.Background()
	var buf = internal.NewAnonymousFile()
	var writer = NewRecordWriter(buf)
	var reader *RecordReader
	var rbuf []byte
	var err error
	var l int

	l, err = writer.Write(ctx, []byte("Hello"))
	if err != nil {
		t.Error("Error writing record: ", err)
	}

	if l != 9 {
		t.Error("Write length mismatched (expected 9, got ", l, ")")
	}

	if buf.Len() != 9 {
		t.Error("Expected length to be 9, was ", buf.Len())
	}

	l, err = writer.Write(ctx, []byte("World"))
	if err != nil {
		t.Error("Error writing record: ", err)
	}

	if l != 9 {
		t.Error("Write length mismatched (expected 9, got ", l, ")")
	}

	if buf.Len() != 18 {
		t.Error("Expected length to be 18, was ", buf.Len())
	}

	// Reset position.
	writer.Close(ctx)

	reader = NewRecordReader(buf)
	rbuf = make([]byte, 20)

	l, err = reader.Read(ctx, rbuf)
	if err != nil {
		t.Error("Error reading record: ", err)
	}

	if l != 5 {
		t.Error("Read length mismatched (expected 5, got ", l, ")")
	}

	if string(rbuf[0:l]) != "Hello" {
		t.Error("Unexpected data: got ", string(rbuf), " (", rbuf,
			"), expected Hello")
	}

	rbuf = make([]byte, 20)

	l, err = reader.Read(ctx, rbuf)
	if err != nil {
		t.Error("Error reading record: ", err)
	}

	if l != 5 {
		t.Error("Read length mismatched (expected 5, got ", l, ")")
	}

	if string(rbuf[0:l]) != "World" {
		t.Error("Unexpected data: got ", string(rbuf), " (", rbuf,
			"), expected World")
	}
}

/*
Test the ReadRecord function to see if it returns and encodes records
properly.
*/
func TestSerializeAndReadRecord(t *testing.T) {
	var ctx = context.Background()
	var buf = internal.NewAnonymousFile()
	var writer = NewRecordWriter(buf)
	var reader *RecordReader
	var rbuf []byte
	var err error
	var l int

	l, err = writer.Write(ctx, []byte("Hello"))
	if err != nil {
		t.Error("Error writing record: ", err)
	}

	if l != 9 {
		t.Error("Write length mismatched (expected 9, got ", l, ")")
	}

	if buf.Len() != 9 {
		t.Error("Expected length to be 9, was ", buf.Len())
	}

	l, err = writer.Write(ctx, []byte("World"))
	if err != nil {
		t.Error("Error writing record: ", err)
	}

	if l != 9 {
		t.Error("Write length mismatched (expected 9, got ", l, ")")
	}

	if buf.Len() != 18 {
		t.Error("Expected length to be 18, was ", buf.Len())
	}

	// Reset position.
	writer.Close(ctx)

	reader = NewRecordReader(buf)

	rbuf, err = reader.ReadRecord(ctx)
	if err != nil {
		t.Error("Error reading record: ", err)
	}

	if len(rbuf) != 5 {
		t.Error("Read length mismatched (expected 5, got ", len(rbuf), ")")
	}

	if string(rbuf) != "Hello" {
		t.Error("Unexpected data: got ", string(rbuf), " (", rbuf,
			"), expected Hello")
	}

	rbuf, err = reader.ReadRecord(ctx)
	if err != nil {
		t.Error("Error reading record: ", err)
	}

	if len(rbuf) != 5 {
		t.Error("Read length mismatched (expected 5, got ", len(rbuf), ")")
	}

	if string(rbuf) != "World" {
		t.Error("Unexpected data: got ", string(rbuf), " (", rbuf,
			"), expected World")
	}
}

/*
Test for the protocol buffer integration into recordio.
*/
func TestSerializeAndReadMessage(t *testing.T) {
	var ctx = context.Background()
	var buf = internal.NewAnonymousFile()
	var writer = NewRecordWriter(buf)
	var reader *RecordReader
	var err error

	var data MessageForTest

	data.Message = "Test data"
	err = writer.WriteMessage(ctx, &data)
	if err != nil {
		t.Error("Cannot serialize message: ", err)
	}

	data.Message = "Toast Data"
	err = writer.WriteMessage(ctx, &data)
	if err != nil {
		t.Error("Cannot serialize message: ", err)
	}

	// Reset position
	writer.Close(ctx)
	reader = NewRecordReader(buf)
	data.Reset()

	err = reader.ReadMessage(ctx, &data)
	if err != nil {
		t.Error("Unable to re-read the message: ", err)
	}

	if data.Message != "Test data" {
		t.Errorf("Expected: Test data, got: %s", data.Message)
	}
	data.Reset()

	err = reader.ReadMessage(ctx, &data)
	if err != nil {
		t.Error("Unable to re-read the message: ", err)
	}

	if data.Message != "Toast Data" {
		t.Errorf("Expected: Toast Data, got: %s", data.Message)
	}
}

/*
Write a bunch of records to a memory buffer and read them back.
Essentially, desperately shouting "Hello" into the void some thousand times.
*/
func BenchmarkRecordWriterAndReader(b *testing.B) {
	var ctx = context.Background()
	var buf = internal.NewAnonymousFile()
	var writer = NewRecordWriter(buf)
	var reader *RecordReader
	var rbuf []byte
	var err error
	var i, l int

	b.StartTimer()

	for i = 0; i < b.N; i++ {
		l, err = writer.Write(ctx, []byte("Hello"))
		if err != nil {
			b.Error("Error writing record: ", err)
		}

		if l != 9 {
			b.Error("Write length mismatched (expected 9, got ", l, ")")
		}
	}

	// Reset position
	writer.Close(ctx)
	reader = NewRecordReader(buf)
	for i = 0; i < b.N; i++ {
		rbuf, err = reader.ReadRecord(ctx)
		if err != nil {
			b.Error("Error reading record: ", err)
		}

		if len(rbuf) != 5 {
			b.Error("Read length mismatched (expected 5, got ", len(rbuf), ")")
		}

		if string(rbuf) != "Hello" {
			b.Error("Unexpected data: got ", string(rbuf), " (", rbuf,
				"), expected Hello")
		}
	}

	b.StopTimer()
	b.ReportAllocs()
}
