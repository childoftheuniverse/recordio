Record Based Readers and Writers
================================

Record I/O (recordio) wraps the regular filesystem I/O functionality
(ReadCloser and WriteCloser, with context support) to provide record-based
input/output functions.

Records are defined as increments of data which are supposed to be considered
at the same time, so when data is written as a record, reading it again should
yield the exact same amount of data. This can be anything which can be
expressed as a slice of bytes, but is most frequently used to store protocol
buffers.

Record I/O doesn't make any assumptions about the type of data being stored.
Data can only be read in sequence or using external indices and seeking,
Record I/O doesn't support seeking over more than one record at a time.

Writing Record I/O files
------------------------

The RecordWriter class implements all functionality required to write data to
files (or other WriteClosers) in the form of records. NewRecordWriter can be
used to wrap any WriteCloser to provide RecordWriter functions.

There are 2 modes of writing available:

 - w.Write(context, bytes) writes the given byte slice to the output file as
   a new record. This means that when reading the file from the position
   before the write, ReadRecord() will return exactly the given byte slice.
 - w.WriteMessage(context, protobuf) will write a protocol buffer to the
   output file as a new record; ReadMessage from RecordReader will return
   the same protocol buffer message when called on the same position the write
   started from.

Reading Record I/O files
------------------------

The RecordReader class implements all functionality required to read data from
files (or other ReadClosers) in the form of records. NewRecordReader can be
used to wrap any ReadCloser to provide RecordReader functions.

There are 3 modes of reading available:

 - r.ReadRecord(context) is the simplest form; it reads the next record and
   returns it as a byte slice.
 - r.Read(context, bytes) is a bit more like Read in io.Reader, it copies the
   next record into a specified byte slice, if it fits. This is not too
   efficient, so it is recommended to use ReadRecord() instead.
 - r.ReadMessage(context, protobuf) reads the next records and attempts to
   parse it as a protocol buffer of the type of the one passed in. Obviously,
   any data previously contained in the protocol buffer will be cleared.
