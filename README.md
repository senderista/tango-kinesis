# tango-kinesis

A totally broken prototype of Tango [1] on Amazon Kinesis, in Python, using Cap'n Proto for message serialization. Due to the Kinesis API's bizarre semantics, I doubt that it's possible for this to work at all (GetRecords can return no results even when there are later records in the stream, so it's impossible to catch up to the tail of the log without looping forever). Also, Amazon's rate-limiting policy for GetRecords (5 reads/second/shard) makes it impossible to use for scenarios like Tango with very frequent reads. Anyway, this is a proof-of-concept that a Tango-like interface is possible to implement on top of an existing shared, durable log with very little effort. All future development will be on Kafka.

[1]: http://www.cs.cornell.edu/~taozou/sosp13/tangososp.pdf
