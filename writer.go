package recordio

import (
	"encoding/binary"
	"errors"
	"github.com/childoftheuniverse/filesystem"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
)

/*
RecordWriter wraps a regular WriteCloser to provide record-based output to
a backing storage system (see the filesystem module). As other filesystem
based I/O routines, RecordWriter also respects any context passed in to the
actual operations.

RecordWriters are not thread safe, so they should be used under locks whenever
they are used in a potentially multi-threaded environment.
*/
type RecordWriter struct {
	filesystem.WriteCloser
	wrappedWriter filesystem.WriteCloser
}

/*
NewRecordWriter creates a new RecordWriter wrapped around the specified
output stream. No actions are performed at the time.
*/
func NewRecordWriter(writer filesystem.WriteCloser) *RecordWriter {
	return &RecordWriter{
		wrappedWriter: writer,
	}
}

/*
Write takes the slice of bytes passed in and writes them to the wrapped output
stream as a new record. This will issue two calls to the Write() method of the
underlying output stream which might conflict, so use locking as appropriate.

This will add len(rec) + 4 bytes to the output stream.
*/
func (w *RecordWriter) Write(ctx context.Context, rec []byte) (int, error) {
	var lengthAsBytes []byte = make([]byte, 4)
	var headerLength int
	var bodyLength int
	var err error

	binary.BigEndian.PutUint32(lengthAsBytes, uint32(len(rec)))

	headerLength, err = w.wrappedWriter.Write(ctx, lengthAsBytes)
	if err != nil {
		return headerLength, err
	}

	bodyLength, err = w.wrappedWriter.Write(ctx, rec)
	if err != nil {
		return headerLength + bodyLength, err
	}

	if bodyLength < len(rec) {
		return headerLength + bodyLength, errors.New("Short write")
	}

	return headerLength + bodyLength, nil
}

/*
WriteMessage serializes the specified protocol buffer to bytes and writes the
result as a new record to the underlying output stream.

The same warnings about locking as for Write() apply to this method.
*/
func (w *RecordWriter) WriteMessage(
	ctx context.Context, pb proto.Message) error {
	var b []byte
	var err error

	b, err = proto.Marshal(pb)
	if err != nil {
		return err
	}

	_, err = w.Write(ctx, b)
	return err
}

/*
Close just delegates to the close function of the underlying writer. No other
specific action will be taken.
*/
func (w *RecordWriter) Close(ctx context.Context) error {
	return w.wrappedWriter.Close(ctx)
}
