package recordio

import (
	"encoding/binary"
	"errors"
	"github.com/childoftheuniverse/filesystem"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
)

/*
RecordReader wraps a ReadCloser to read data from an input stream. The data
returned will be split into records.

Since the length of the next record is always encoded before the data, this
must only be used on trusted data. Never use this class to read user-defined
data!
*/
type RecordReader struct {
	filesystem.ReadCloser
	wrappedReader filesystem.ReadCloser
}

/*
NewRecordReader creates a new RecordReader wrapped around the specified
input stream. No actions are performed at the time.
*/
func NewRecordReader(reader filesystem.ReadCloser) *RecordReader {
	return &RecordReader{
		wrappedReader: reader,
	}
}

/*
ReadRecord() reads the next record from the input stream and returns it to the
caller.

This will read the length of the upcoming record first (4 bytes), which will
be used to size the buffer. Therefor, this function must only be called on
trusted data which is known to be a RecordWriter compatible stream. Also, the
stream should be pointed at the beginning of a record. Otherwise, large
amounts of memory may be allocated for no good reason, and the result is
probably going to be garbage.
*/
func (r *RecordReader) ReadRecord(ctx context.Context) ([]byte, error) {
	var rec []byte
	var lengthAsBytes []byte = make([]byte, 4)
	var headerLength int
	var bodyLength uint32
	var lengthRead int
	var err error

	headerLength, err = r.wrappedReader.Read(ctx, lengthAsBytes)
	if err != nil {
		return []byte{}, err
	}

	if headerLength != 4 {
		return []byte{}, errors.New("Short read for header")
	}

	bodyLength = binary.BigEndian.Uint32(lengthAsBytes)
	rec = make([]byte, bodyLength)
	lengthRead, err = r.wrappedReader.Read(ctx, rec)
	if err == nil && uint32(lengthRead) < bodyLength {
		err = errors.New("Short read for body")
	}

	return rec, err
}

/*
For filesystem.ReadCloser compatibility. This will read the next record and
place it into the specified buffer. This will only ever read data the size of
the following record.

If the buffer is too small to hold the data, an error will be returned and no
data will be placed into the buffer. The reader will still be advanced by one
record.

All warnings from the ReadRecord() method apply here as well.
*/
func (r *RecordReader) Read(ctx context.Context, buffer []byte) (int, error) {
	var internalBuffer []byte
	var err error

	internalBuffer, err = r.ReadRecord(ctx)
	if err != nil {
		return 0, err
	}

	if len(internalBuffer) > cap(buffer) {
		return 0, errors.New("Insufficiently large buffer")
	}

	copy(buffer, internalBuffer)

	return len(internalBuffer), nil
}

/*
ReadMessage reads the next record from the input stream and attempts to parse
it as a protocol buffer message. The result will be placed into the protocol
buffer passed in as a parameter.

Setting the type of protocol buffer messages is simply done by passing in a
protocol buffer of the correct type. If the record cannot be parsed as the
specified protocol buffer type, an error will be returned but the reader will
be advanced by a record.

All warnings from the ReadRecord() method apply here as well.
*/
func (r *RecordReader) ReadMessage(ctx context.Context, pb proto.Message) error {
	var buf []byte
	var err error

	buf, err = r.ReadRecord(ctx)
	if err != nil {
		return err
	}

	return proto.Unmarshal(buf, pb)
}
