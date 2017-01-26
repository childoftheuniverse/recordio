/*
Record I/O enhances the I/O support framework form the filesystem code
by adding record based I/O functionality. Records are sets of data with a
certain length which will be respected both when reading and writing them.

This means that every call to Write() will produce a new, individual
record, and that the corresponding call to Read() will return exactly
the data which was sent to Write().

Protocol buffers are supported as a special kind of message. The length
of the record will be the length of the record.
*/
package recordio
