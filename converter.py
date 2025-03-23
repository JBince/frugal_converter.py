import struct
import json
import base64
from string import Formatter
import argparse
from thrift_tools.thrift_message import ThriftMessage, ThriftStruct
from thrift.Thrift import TType, TMessageType
from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.protocol.TCompactProtocol import TCompactProtocol
from thrift.transport.TTransport import TMemoryBuffer
from classes import ThriftJsonEncoder, CustomResponseMessage

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

def parse_data(filename, markers=None):
    if markers is None:
        # Define a comprehensive list of potential markers
        markers = [
            b'\x80\x01',         # Standard Thrift binary protocol
            b'\x82\x21',         # Another common variant
            b'\x80\x01\x00\x01', # TFramedTransport with call type
            b'\x80\x01\x00\x02', # TFramedTransport with reply type
            b'\x80\x01\x00\x03', # TFramedTransport with exception type
            b'\x80\x01\x00\x04', # TFramedTransport with oneway type
        ]
    
    with open(filename, 'rb') as f:
        base_64_data = f.read()
        data = base64.b64decode(base_64_data)
        
        # First try to detect a Frugal message structure (version byte + header length)
        if len(data) > 9:
            try:
                # Check if we can interpret the first 9 bytes as Frugal header metadata
                frame_size = struct.unpack('>I', data[:4])[0]
                version = data[4]
                header_length = struct.unpack('>I', data[5:9])[0]
                
                # Sanity check frame size and header length 
                if (0 < frame_size < len(data) and 
                    0 <= version <= 1 and 
                    header_length < frame_size):
                    # Looks like a Frugal message - get the Thrift part
                    header_data = data[:9+header_length]
                    thrift_frame = data[9+header_length:]
                    return header_data, thrift_frame
            except:
                # Not a Frugal message, continue with marker detection
                pass
                
        # Search for marker positions
        marker_positions = []
        for marker in markers:
            pos = data.find(marker)
            if pos != -1:
                marker_positions.append((pos, marker))
        
        if not marker_positions:
            # No standard markers found - try a byte-by-byte scan for protocol markers
            for i in range(len(data) - 2):
                # Look for bytes that could indicate a protocol header
                if ((data[i] == 0x80 and data[i+1] == 0x01) or 
                    (data[i] == 0x82 and data[i+1] == 0x21)):
                    marker_positions.append((i, data[i:i+2]))
        
        if not marker_positions:
            raise ValueError('No Thrift/Frugal marker found in the message')
        
        # Sort positions to start with the earliest marker
        marker_positions.sort()
        
        # Return using the first marker found
        pos, marker = marker_positions[0]
        header_data = data[:pos]
        thrift_frame = data[pos:]
        
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
    # First try direct response detection and handling
    if len(thrift_frame) >= 4 and (thrift_frame[:4] == b'\x80\x01\x00\x02' or 
                                  thrift_frame[:4] == b'\x82\x21\x00\x02'):
        print("Detected response message directly")
        try:
            # Use our custom response message handler
            return CustomResponseMessage(thrift_frame)
        except Exception as e:
            print(f"Error parsing response with custom handler: {e}")
    
    # Try with different markers for requests
    candidates = [
        b'\x80\x01\x00\x01',  # Call message
        b'\x80\x01\x00\x02',  # Reply message
        b'\x82\x21\x00\x01',  # Variant call
        b'\x82\x21\x00\x02',  # Variant reply
        b'\x80\x01',          # Just protocol marker
        b'\x82\x21',          # Variant protocol marker
    ]
    
    for candidate in candidates:
        try:
            idx = thrift_frame.find(candidate)
            if idx != -1:
                # Get the frame starting at the marker
                frame_slice = thrift_frame[idx:]
                
                try:
                    # Check if this is a response marker
                    if candidate in [b'\x80\x01\x00\x02', b'\x82\x21\x00\x02']:
                        return CustomResponseMessage(frame_slice)
                    
                    # Otherwise try normal request parsing
                    try:
                        msg, msglen = ThriftMessage.read(frame_slice, read_values=True)
                        if msg:
                            return msg
                    except TypeError as te:
                        if "unhashable type: 'ThriftStruct'" in str(te):
                            print("Caught unhashable ThriftStruct error, using custom handler")
                            # If we get the unhashable error, try our custom handler
                            return CustomResponseMessage(frame_slice)
                        else:
                            raise te
                except Exception as e:
                    print(f"Error parsing with marker {candidate.hex()}: {e}")
                    continue
        except Exception as e:
            print(f"Error finding marker {candidate.hex()}: {e}")
    
    # If all else fails, return an empty message
    print("=======================================")
    print("FAILED TO DECODE THRIFT MESSAGE")
    print(f"Frame length: {len(thrift_frame)} bytes")
    print(f"First 20 bytes: {thrift_frame[:20].hex()}")
    print("=======================================")
    
    return create_empty_thrift_message()

def create_empty_thrift_message():
    class EmptyThriftMessage:
        def __init__(self):
            self.as_dict = {
                "method": "unknown",
                "type": "unknown",
                "seqid": 0,
                "args": None,
                "length": 0,
                "error": "Failed to parse Thrift message"
            }
    
    return EmptyThriftMessage()

def decode(filename):
    try:
        raw_headers, raw_thrift_frame = parse_data(filename)
        message = decode_headers(raw_headers)
        
        # Try to decode the Thrift message, will never return None anymore
        thrift_message = decode_thrift_message(raw_thrift_frame)
        message['thrift'] = thrift_message.as_dict
        
        # Add information about whether the thrift decoding was successful
        if "error" in thrift_message.as_dict:
            message['thrift_parse_error'] = True
        
        if args.output:
            with open(args.output, 'w') as f:
                f.write(json.dumps(message, cls=ThriftJsonEncoder, indent=4))
        else:
            print(json.dumps(message, cls=ThriftJsonEncoder, indent=4))
            
    except Exception as e:
        error_info = {
            "error": f"Failed to decode message: {str(e)}",
            "metadata": {},
            "headers": {},
            "thrift": {
                "method": "unknown",
                "type": "unknown",
                "seqid": 0,
                "args": None
            }
        }
        
        if args.output:
            with open(args.output, 'w') as f:
                f.write(json.dumps(error_info, indent=4))
        else:
            print(json.dumps(error_info, indent=4))

# Encoding Functions. There is a marginal difference in the current output vs the original request towards the final bytes, but it does still seem to work correctly.
def write_value(protocol, thrift_type, field):
    # Extract the value from the field dictionary
    value = field.get("value")
    
    if thrift_type == TType.BOOL:
        protocol.writeBool(bool(value) if value is not None else False)
    elif thrift_type == TType.BYTE:
        protocol.writeByte(int(value) if value is not None else 0)
    elif thrift_type == TType.I16:
        protocol.writeI16(int(value) if value is not None else 0)
    elif thrift_type == TType.I32:
        protocol.writeI32(int(value) if value is not None else 0)
    elif thrift_type == TType.I64:
        protocol.writeI64(int(value) if value is not None else 0)
    elif thrift_type == TType.DOUBLE:
        protocol.writeDouble(float(value) if value is not None else 0.0)
    elif thrift_type == TType.STRING:
        # Ensure the value is a string before encoding
        if value is None:
            value = ""
        elif not isinstance(value, str):
            value = str(value)
        protocol.writeString(value)
    elif thrift_type == TType.STRUCT:
        # Get the fields from the value dictionary
        if value is None or not value:
            # Handle empty struct
            protocol.writeStructBegin("struct")
            protocol.writeFieldStop()
            protocol.writeStructEnd()
        else:
            fields_json = value.get("fields", [])
            write_struct(protocol, fields_json)
    elif thrift_type == TType.MAP:
        # Extract map information
        key_type_str = field.get("key_type")
        value_type_str = field.get("value_type")
        
        # If key_type and value_type aren't specified, default to string
        if not key_type_str:
            key_type_str = "string"
        if not value_type_str:
            value_type_str = "string"
        
        # Determine key and value types
        key_type = FIELD_TYPE_MAP.get(key_type_str)
        value_type = FIELD_TYPE_MAP.get(value_type_str)
        
        # Get the map entries
        if value is None:
            entries = []
        elif isinstance(value, dict):
            # Convert plain dictionary to entries format
            entries = [{"key": k, "value": v} for k, v in value.items()]
        else:
            entries = value or []
        
        # Begin map writing
        protocol.writeMapBegin(key_type, value_type, len(entries))
        
        # Write each key-value pair
        for entry in entries:
            if isinstance(entry, dict) and "key" in entry and "value" in entry:
                key = entry["key"]
                val = entry["value"]
                
                # Write key
                write_field_value(protocol, key_type, key)
                
                # Write value
                write_field_value(protocol, value_type, val)
        
        # End map writing
        protocol.writeMapEnd()
    elif thrift_type == TType.SET:
        # Get list of items or empty list if None
        items = value or []
        
        # If we have a list of strings/primitives or a mix, handle appropriately
        if all(isinstance(item, (str, int, float, bool)) for item in items):
            # For primitive types, assume string if we don't have element_type
            element_type = TType.STRING
            protocol.writeSetBegin(element_type, len(items))
            for item in items:
                write_field_value(protocol, element_type, item)
        else:
            # For complex types, assume STRUCT
            element_type = TType.STRUCT
            protocol.writeSetBegin(element_type, len(items))
            for item in items:
                if isinstance(item, dict) and "fields" in item:
                    write_struct(protocol, item["fields"])
                else:
                    # Try to handle item as is
                    write_field_value(protocol, element_type, item)
        protocol.writeSetEnd()
    elif thrift_type == TType.LIST:
        element_type_str = field.get("element_type")
        
        # If element_type is missing, try to infer from content
        if not element_type_str:
            # Get list of items or empty list if None
            items = value or []
            
            # Try to infer element type from first item if available
            if items and all(isinstance(item, (str, int, float, bool)) for item in items):
                if all(isinstance(item, bool) for item in items):
                    element_type = TType.BOOL
                elif all(isinstance(item, int) for item in items):
                    element_type = TType.I32
                elif all(isinstance(item, float) for item in items):
                    element_type = TType.DOUBLE
                else:
                    element_type = TType.STRING
            else:
                # Default to STRUCT for complex or mixed types
                element_type = TType.STRUCT
        else:
            # Determine the element type from the provided string
            element_type = FIELD_TYPE_MAP.get(element_type_str)
            if element_type is None:
                raise ValueError(f"Unsupported list element type: {element_type_str}")
        
        # Get list of items or empty list if None
        items = value or []
        
        protocol.writeListBegin(element_type, len(items))
        for item in items:
            if element_type == TType.STRUCT:
                if isinstance(item, dict) and "fields" in item:
                    write_struct(protocol, item["fields"])
                else:
                    # Try to handle non-standard struct format
                    write_field_value(protocol, element_type, item)
            else:
                write_field_value(protocol, element_type, item)
        protocol.writeListEnd()
    else:
        raise ValueError(f"Unsupported thrift type: {thrift_type}")
    

def write_field_value(protocol, thrift_type, value):
    """Helper function to write a value based on its type without the field structure."""
    if thrift_type == TType.BOOL:
        protocol.writeBool(bool(value))
    elif thrift_type == TType.BYTE:
        protocol.writeByte(int(value))
    elif thrift_type == TType.I16:
        protocol.writeI16(int(value))
    elif thrift_type == TType.I32:
        protocol.writeI32(int(value))
    elif thrift_type == TType.I64:
        protocol.writeI64(int(value))
    elif thrift_type == TType.DOUBLE:
        protocol.writeDouble(float(value))
    elif thrift_type == TType.STRING:
        # Ensure the value is a string before encoding
        if value is None:
            value = ""
        elif not isinstance(value, str):
            value = str(value)
        protocol.writeString(value)
    elif thrift_type == TType.STRUCT:
        if "fields" in value:
            write_struct(protocol, value["fields"])
        else:
            # If it's not in the expected format, write an empty struct
            protocol.writeStructBegin("struct")
            protocol.writeFieldStop()
            protocol.writeStructEnd()
    else:
        raise ValueError(f"Unsupported simple thrift type: {thrift_type}")
    
def write_simple_value(protocol, thrift_type, value):
    if thrift_type == TType.BOOL:
        protocol.writeBool(bool(value))
    elif thrift_type == TType.BYTE:
        protocol.writeByte(int(value))
    elif thrift_type == TType.I16:
        protocol.writeI16(int(value))
    elif thrift_type == TType.I32:
        protocol.writeI32(int(value))
    elif thrift_type == TType.I64:
        protocol.writeI64(int(value))
    elif thrift_type == TType.DOUBLE:
        protocol.writeDouble(float(value))
    elif thrift_type == TType.STRING:
        protocol.writeString(value)
    else:
        raise ValueError(f"Unsupported simple thrift type in list: {thrift_type}")

def write_struct(protocol, fields_json):
    protocol.writeStructBegin("struct")
    for field in fields_json:
        field_id = field["field_id"]
        ftype_str = field["field_type"]
        ftype = FIELD_TYPE_MAP[ftype_str]
        
        # Write the field header (id and type)
        protocol.writeFieldBegin("field", ftype, field_id)
        
        # Write the field value
        write_value(protocol, ftype, field)
        
        protocol.writeFieldEnd()
    protocol.writeFieldStop()
    protocol.writeStructEnd()

def write_thrift_message(thrift_json):
    method_name = thrift_json["method"]
    msg_type_str = thrift_json["type"]
    seqid = thrift_json["seqid"]

    msg_type_map = {
        "call": TMessageType.CALL,
        "reply": TMessageType.REPLY,
        "exception": TMessageType.EXCEPTION,
        "oneway": TMessageType.ONEWAY
    }

    msg_type = msg_type_map[msg_type_str]
    transport = TMemoryBuffer()
    if args.compact:
        protocol = TCompactProtocol(transport)
    else:
        protocol = TBinaryProtocol(transport)

    protocol.writeMessageBegin(method_name, msg_type, seqid)
    if thrift_json["args"]:
        fields_json = thrift_json["args"]["fields"]
        write_struct(protocol, fields_json)
    protocol.writeMessageEnd()
    protocol.trans.flush()

    return transport.getvalue()

# Rewrite to include header encoding
def encode_data(filename):
    message = b''
    headers = b''
    thrift_message = b''
    with open(filename, 'r') as f:
        data = f.read()
        x = json.loads(data)
        # Build headers..
        for i in x['headers']:
            # Write the length of the key
            headers += struct.pack('>I', len(i))
            # Write the key
            headers += i.encode('utf-8')
            # Write the length of the value
            headers += struct.pack('>I', len(x['headers'][i]))
            # Write the value
            headers += x['headers'][i].encode('utf-8')

        
        # Build Thrift Message
        thrift_data = x["thrift"]
        thrift_message = write_thrift_message(thrift_data)
        
        # Calculate message and header lengths
        header_length = len(headers)
        # Must be adjusted to include header version and header length size, 5 bytes
        frame_size = len(headers) + len(thrift_message) + 5
        # Entire message should be 2461
        
        # Header length should be 2301

        # Build the Frugal message
        message += struct.pack('>I', frame_size)
        message += struct.pack('>B', 0)
        message += struct.pack('>I', header_length)
        message += headers
        message += thrift_message

        encoded_message = base64.b64encode(message)
        print(encoded_message.decode('utf-8'))    

if __name__ == '__main__':
    # Arguments
    parser = argparse.ArgumentParser(description='Encode and decode base64 Frugal Messaegs :)')
    parser.add_argument('-f','--filename', required=True, type=str, help='Filename to decode')
    parser.add_argument('-o','--output', required=False, type=str, help='Output file')
    parser.add_argument('-e','--encode', required=False, action='store_true', help='Encode file')
    parser.add_argument('-d','--decode', required=False, action='store_true', help='Decode file')
    parser.add_argument('-c', '--compact', required=False, action='store_true', help='Use compact protocol')
    # Add encoding functionality
    args = parser.parse_args()
    if (args.encode and args.decode) or (not args.encode and not args.decode):
        raise ValueError('Please select either encode or decode')
        exit()
    elif args.encode:
        encode_data(args.filename)
    elif args.decode:
        decode(args.filename)