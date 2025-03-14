import struct
import json
import base64
from string import Formatter
import argparse
from thrift_tools.thrift_message import ThriftMessage, ThriftStruct
from thrift.Thrift import TType, TMessageType
from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.transport.TTransport import TMemoryBuffer

FIELD_TYPE_MAP = {
    "bool": TType.BOOL,
    "i8": TType.BYTE,
    "i16": TType.I16,
    "i32": TType.I32,
    "i64": TType.I64,
    "double": TType.DOUBLE,
    "string": TType.STRING,
    "struct": TType.STRUCT,
    "map": TType.MAP,
    "set": TType.SET,
    "list": TType.LIST
}

# Decoding Functions
class ThriftJsonEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ThriftStruct):
            return o.as_dict
        else:
            return super().default(o)

def parse_data(filename, marker=b'\x80\x01'):
    with open(filename, 'rb') as f:
        base_64_data = f.read()
        data = base64.b64decode(base_64_data)

        # Find the marker in the file data
        pos = data.find(marker)
        if pos == -1:
            raise ValueError('Marker not found')
        
        header_data = data[:pos]
        thrift_frame = data[pos + len(marker)-2:]
        return header_data, thrift_frame
        
def decode_headers(header):
    message = {}
    metadata = {}
    mb = memoryview(header)

    # Get Message Length
    message_length = struct.unpack('>I', mb[:4])
    metadata['message_length'] = message_length[0]

    # Get Header Version (Will always be 0 for now)
    version = struct.unpack('>B', mb[4:5])
    metadata['version'] = version[0]

    # Get header length
    header_length = struct.unpack('>I', mb[5:9])
    metadata['header_length'] = header_length[0]

    # Add metadata of message to Headers
    message['metadata'] = metadata
    

    # Start Parsing Headers
    offset = 9
    data_length = message['metadata']['header_length']
    headers = {}
    while offset < data_length:
        # Get Key Length
        key_length = struct.unpack('>I', mb[offset:offset+4])
        offset += 4

        # Get Key
        key = mb[offset:offset+key_length[0]].tobytes().decode('utf-8')
        offset += key_length[0]

        # Get Value Length
        value_length = struct.unpack('>I', mb[offset:offset+4])
        offset += 4

        # Get Value
        value = mb[offset:offset+value_length[0]].tobytes().decode('utf-8')
        offset += value_length[0]
        

        headers[key] = value
    
    message['headers'] = headers
    return message

# Based on: https://github.com/LAripping/thrift-inspector
def decode_thrift_message(thrift_frame):
    # Iterate through length of message to be certain we have the thrift message
    candidates = [b'\x80\x01\x00\x01',b'\x80\x01\x00\x02',b'\x80\x01\x00\x03',b'\x80\x01\x00\x04']
    for candidate in candidates:
        try:
            idx = thrift_frame.find(candidate)
            if idx != -1:
                thrift_frame = thrift_frame[idx:]
                try:
                    msg, msglen = ThriftMessage.read(thrift_frame, read_values=True)
                except Exception as e:
                    print("Error: ", e)
                    continue
                # print("Message: ", msg)
                return msg
        except Exception as e:
            continue

def decode(filename):
    raw_headers, raw_thrift_frame = parse_data(filename)
    message = decode_headers(raw_headers)
    thrift_message = decode_thrift_message(raw_thrift_frame)
    message['thrift'] = thrift_message.as_dict
    print(json.dumps(message, cls=ThriftJsonEncoder, indent=4))

if __name__ == '__main__':
    decode("requests/get_people.b64")