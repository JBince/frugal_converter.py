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

class ThriftJsonEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ThriftStruct):
            return o.as_dict
        else:
            return super().default(o)

class CustomResponseMessage:
    def __init__(self, thrift_frame):
        # Initialize with the raw thrift frame
        self.thrift_frame = thrift_frame
        self.transport = TMemoryBuffer(thrift_frame)
        self.protocol = TBinaryProtocol(self.transport)
        
        # Read message header
        name, msg_type, seqid = self.protocol.readMessageBegin()
        
        # Create the response dictionary
        self.as_dict = {
            "method": name,
            "type": "reply",
            "seqid": seqid,
            "header": None,
            "reply": self._extract_reply(self.protocol),
            "length": len(thrift_frame)
        }
    
    def _extract_reply(self, protocol):
        """Extract reply data with full handling for complex structs"""
        try:
            # Create a dict representation of the struct
            result = {"fields": []}
            
            # Read struct begin
            protocol.readStructBegin()
            
            # Read fields until we hit STOP
            while True:
                field_name, field_type, field_id = protocol.readFieldBegin()
                if field_type == TType.STOP:
                    break
                    
                # Extract field based on type
                field = {
                    "field_id": field_id,
                    "field_type": self._get_type_name(field_type)
                }
                
                # Handle field based on type
                if field_type == TType.BOOL:
                    field["value"] = protocol.readBool()
                elif field_type == TType.BYTE:
                    field["value"] = protocol.readByte()
                elif field_type == TType.I16:
                    field["value"] = protocol.readI16()
                elif field_type == TType.I32:
                    field["value"] = protocol.readI32()
                elif field_type == TType.I64:
                    field["value"] = protocol.readI64()
                elif field_type == TType.DOUBLE:
                    field["value"] = protocol.readDouble()
                elif field_type == TType.STRING:
                    field["value"] = protocol.readString()
                elif field_type == TType.STRUCT:
                    # Fully parse nested struct
                    field["value"] = self._read_struct(protocol)
                elif field_type == TType.LIST:
                    field["value"] = self._read_list(protocol)
                elif field_type == TType.SET:
                    field["value"] = self._read_set(protocol)
                elif field_type == TType.MAP:
                    field["value"] = self._read_map(protocol)
                else:
                    # Unknown type - skip
                    field["value"] = {"note": f"Unknown type {field_type} (skipped)"}
                    protocol.skip(field_type)
                    
                result["fields"].append(field)
                protocol.readFieldEnd()
                
            protocol.readStructEnd()
            return result
        except Exception as e:
            print(f"Error extracting reply: {e}")
            return {"error": str(e)}
            
    def _read_struct(self, protocol):
        """Read a nested struct - fully recursive"""
        struct_data = {"fields": []}
        struct_name = protocol.readStructBegin()
        
        while True:
            field_name, field_type, field_id = protocol.readFieldBegin()
            if field_type == TType.STOP:
                break
                
            field = {
                "field_id": field_id,
                "field_type": self._get_type_name(field_type)
            }
            
            # Handle field based on type - fully recursive now
            if field_type == TType.BOOL:
                field["value"] = protocol.readBool()
            elif field_type == TType.BYTE:
                field["value"] = protocol.readByte()
            elif field_type == TType.I16:
                field["value"] = protocol.readI16()
            elif field_type == TType.I32:
                field["value"] = protocol.readI32()
            elif field_type == TType.I64:
                field["value"] = protocol.readI64()
            elif field_type == TType.DOUBLE:
                field["value"] = protocol.readDouble()
            elif field_type == TType.STRING:
                field["value"] = protocol.readString()
            elif field_type == TType.STRUCT:
                # Fully recursive struct handling
                field["value"] = self._read_struct(protocol)
            elif field_type == TType.LIST:
                field["value"] = self._read_list(protocol)
            elif field_type == TType.SET:
                field["value"] = self._read_set(protocol)
            elif field_type == TType.MAP:
                field["value"] = self._read_map(protocol)
            else:
                field["value"] = {"note": f"Unknown type {field_type}"}
                protocol.skip(field_type)
                
            struct_data["fields"].append(field)
            protocol.readFieldEnd()
            
        protocol.readStructEnd()
        return struct_data

    def _read_list(self, protocol):
        """Read a list - fully recursive"""
        element_type, size = protocol.readListBegin()
        result = []
        
        for i in range(size):
            if element_type == TType.BOOL:
                result.append(protocol.readBool())
            elif element_type == TType.BYTE:
                result.append(protocol.readByte())
            elif element_type == TType.I16:
                result.append(protocol.readI16())
            elif element_type == TType.I32:
                result.append(protocol.readI32())
            elif element_type == TType.I64:
                result.append(protocol.readI64())
            elif element_type == TType.DOUBLE:
                result.append(protocol.readDouble())
            elif element_type == TType.STRING:
                result.append(protocol.readString())
            elif element_type == TType.STRUCT:
                # Fully recursive struct in list
                result.append(self._read_struct(protocol))
            elif element_type == TType.LIST:
                # Nested list
                result.append(self._read_list(protocol))
            elif element_type == TType.SET:
                # Nested set
                result.append(self._read_set(protocol))
            elif element_type == TType.MAP:
                # Nested map
                result.append(self._read_map(protocol))
            else:
                # For unknown types, add placeholder
                result.append({"note": f"Unknown element type {self._get_type_name(element_type)}"})
                protocol.skip(element_type)
        
        protocol.readListEnd()
        return result

    def _read_set(self, protocol):
        """Read a set - fully recursive"""
        element_type, size = protocol.readSetBegin()
        result = []
        
        for i in range(size):
            if element_type == TType.BOOL:
                result.append(protocol.readBool())
            elif element_type == TType.BYTE:
                result.append(protocol.readByte())
            elif element_type == TType.I16:
                result.append(protocol.readI16())
            elif element_type == TType.I32:
                result.append(protocol.readI32())
            elif element_type == TType.I64:
                result.append(protocol.readI64())
            elif element_type == TType.DOUBLE:
                result.append(protocol.readDouble())
            elif element_type == TType.STRING:
                result.append(protocol.readString())
            elif element_type == TType.STRUCT:
                # Fully recursive struct in set
                result.append(self._read_struct(protocol))
            elif element_type == TType.LIST:
                # Nested list
                result.append(self._read_list(protocol))
            elif element_type == TType.SET:
                # Nested set (rare)
                result.append(self._read_set(protocol))
            elif element_type == TType.MAP:
                # Nested map
                result.append(self._read_map(protocol))
            else:
                result.append({"note": f"Unknown element type {self._get_type_name(element_type)}"})
                protocol.skip(element_type)
        
        protocol.readSetEnd()
        return result

    def _read_map(self, protocol):
        """Read a map - fully recursive"""
        key_type, value_type, size = protocol.readMapBegin()
        result = {}
        
        for i in range(size):
            # Read key based on type
            if key_type == TType.BOOL:
                key = str(protocol.readBool())
            elif key_type == TType.BYTE:
                key = str(protocol.readByte())
            elif key_type == TType.I16:
                key = str(protocol.readI16())
            elif key_type == TType.STRING:
                key = protocol.readString()
            elif key_type == TType.I32:
                key = str(protocol.readI32())
            elif key_type == TType.I64:
                key = str(protocol.readI64())
            else:
                # For complex keys, use placeholder
                key = f"complex_key_{i}"
                protocol.skip(key_type)
            
            # Read value based on type - fully recursive
            if value_type == TType.BOOL:
                result[key] = protocol.readBool()
            elif value_type == TType.BYTE:
                result[key] = protocol.readByte()
            elif value_type == TType.I16:
                result[key] = protocol.readI16()
            elif value_type == TType.I32:
                result[key] = protocol.readI32()
            elif value_type == TType.I64:
                result[key] = protocol.readI64()
            elif value_type == TType.DOUBLE:
                result[key] = protocol.readDouble()
            elif value_type == TType.STRING:
                result[key] = protocol.readString()
            elif value_type == TType.STRUCT:
                # Fully recursive struct in map
                result[key] = self._read_struct(protocol)
            elif value_type == TType.LIST:
                # Nested list
                result[key] = self._read_list(protocol)
            elif value_type == TType.SET:
                # Nested set
                result[key] = self._read_set(protocol)
            elif value_type == TType.MAP:
                # Nested map
                result[key] = self._read_map(protocol)
            else:
                result[key] = {"note": f"Unknown value type {self._get_type_name(value_type)}"}
                protocol.skip(value_type)
        
        protocol.readMapEnd()
        return result
    
    def _get_type_name(self, type_id):
        """Convert type ID to name"""
        type_names = {
            TType.BOOL: "bool",
            TType.BYTE: "i8",
            TType.I16: "i16",
            TType.I32: "i32",
            TType.I64: "i64",
            TType.DOUBLE: "double",
            TType.STRING: "string",
            TType.STRUCT: "struct",
            TType.MAP: "map",
            TType.SET: "set",
            TType.LIST: "list"
        }
        return type_names.get(type_id, f"unknown-{type_id}")