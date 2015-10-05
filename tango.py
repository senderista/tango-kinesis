import sys
from time import sleep
import boto.kinesis
from collections import MutableMapping
import capnp
import tango_capnp

class TangoRuntime(object):
    def __init__(self, region, stream_name):
        self.stream_name = stream_name
        self.shard_id = None
        self.last_seq_num = None
        self.shard_iterator = None
        self.kinesis = boto.kinesis.connect_to_region(region, profile_name='personal')
        response = self.kinesis.describe_stream(self.stream_name)
        if response['StreamDescription']['StreamStatus'] == 'ACTIVE':
            self.shard_id = response['StreamDescription']['Shards'][0]['ShardId']
        else:
            raise 'Stream not active, aborting...'

    def query_helper(self, obj):
        # catch up to tail of log and apply all pending updates
        stop_on_empty_response = False
        if self.last_seq_num is None:
            response = self.kinesis.get_shard_iterator(self.stream_name, self.shard_id, 'TRIM_HORIZON')
        else:
            stop_on_empty_response = True
            response = self.kinesis.get_shard_iterator(
                self.stream_name, self.shard_id, 'AFTER_SEQUENCE_NUMBER', starting_sequence_number=self.last_seq_num)
        self.shard_iterator = response['ShardIterator']
        iterations = 0
        while True:
            response = self.kinesis.get_records(self.shard_iterator)
            self.shard_iterator = response['NextShardIterator']
            # when using TRIM_HORIZON iterator, GetRecords can return empty many times before returning results
            if stop_on_empty_response and not response['Records']:
                break
            for record in response['Records']:
                stop_on_empty_response = True
                self.last_seq_num = record['SequenceNumber']
                encoded_record = str(record['Data'])
                obj.apply(encoded_record)
            sleep(1)
            iterations += 1

    def update_helper(self, data):
        response = self.kinesis.put_record(
            self.stream_name, data, "0", sequence_number_for_ordering=self.last_seq_num)
        self.last_seq_num = response['SequenceNumber']


class TangoMap(MutableMapping):
    def __init__(self, runtime):
        self.runtime = runtime
        self.dict = dict()

    def apply(self, encoded_record):
        tango_record = tango_capnp.TangoMapRecord.from_bytes(encoded_record)
        which = tango_record.payload.which()
        if which == 'update':
            self.dict[tango_record.payload.update.key] = tango_record.payload.update.value
        elif which == 'delete':
            del self.dict[tango_record.payload.delete.key]
        else:
            raise UnknownRecordType()

    def make_update_record(self, key, value):
        update_record = tango_capnp.TangoMapRecord.new_message()
        update = update_record.payload.init('update')
        update.key = key
        update.value = value
        return update_record.to_bytes()

    def make_delete_record(self, key):
        delete_record = tango_capnp.TangoMapRecord.new_message()
        delete = delete_record.payload.init('delete')
        delete.key = key
        return delete_record.to_bytes()

    def __getitem__(self, key):
        self.runtime.query_helper(self)
        return self.dict.get(key)

    def __setitem__(self, key, value):
        self.runtime.update_helper(self.make_update_record(key, value))
        self.dict[key] = value

    def __delitem__(self, key):
        self.runtime.update_helper(self.make_delete_record(key))
        del self.dict[key]

    def __iter__(self):
        self.runtime.query_helper(self)
        return iter(self.dict)

    def __len__(self):
        self.runtime.query_helper(self)
        return len(self.dict)


class UnknownRecordType(Exception):
    pass

if __name__ == "__main__":
    region, stream = sys.argv[1:3]
    runtime = TangoRuntime(region, stream)
    tango_map = TangoMap(runtime)
    for k, v in tango_map.iteritems():
        print k, v
