# -*- coding: utf-8 -*-
# Generado por el compilador del búfer de protocolo. ¡NO EDITAR!
# NO CHECKED-IN PROTOBUF GENCODE
# source: client.proto
# Protobuf Python Version: 6.31.0
"""Código de búfer de protocolo generado."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(
    _runtime_version.Domain.PUBLIC,
    6,
    31,
    0,
    '',
    'client.proto'
)
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0c\x63lient.proto\x12\x07kvstore\"(\n\nSetRequest\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\x0c\".\n\x0bSetResponse\x12\x0e\n\x06status\x18\x01 \x01(\x08\x12\x0f\n\x07message\x18\x02 \x01(\t\"\x19\n\nGetRequest\x12\x0b\n\x03key\x18\x01 \x01(\t\"+\n\x0bGetResponse\x12\r\n\x05\x66ound\x18\x01 \x01(\x08\x12\r\n\x05value\x18\x02 \x01(\x0c\"\"\n\x10GetPrefixRequest\x12\x0e\n\x06prefix\x18\x01 \x01(\t\"#\n\x11GetPrefixResponse\x12\x0e\n\x06values\x18\x01 \x03(\x0c\x32\xbd\x01\n\rKeyValueStore\x12\x32\n\x03Set\x12\x13.kvstore.SetRequest\x1a\x14.kvstore.SetResponse\"\x00\x12\x32\n\x03Get\x12\x13.kvstore.GetRequest\x1a\x14.kvstore.GetResponse\"\x00\x12\x44\n\tGetPrefix\x12\x19.kvstore.GetPrefixRequest\x1a\x1a.kvstore.GetPrefixResponse\"\x00\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'client_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
  DESCRIPTOR._loaded_options = None
  _globals['_SETREQUEST']._serialized_start=25
  _globals['_SETREQUEST']._serialized_end=65
  _globals['_SETRESPONSE']._serialized_start=67
  _globals['_SETRESPONSE']._serialized_end=113
  _globals['_GETREQUEST']._serialized_start=115
  _globals['_GETREQUEST']._serialized_end=140
  _globals['_GETRESPONSE']._serialized_start=142
  _globals['_GETRESPONSE']._serialized_end=185
  _globals['_GETPREFIXREQUEST']._serialized_start=187
  _globals['_GETPREFIXREQUEST']._serialized_end=221
  _globals['_GETPREFIXRESPONSE']._serialized_start=223
  _globals['_GETPREFIXRESPONSE']._serialized_end=258
  _globals['_KEYVALUESTORE']._serialized_start=261
  _globals['_KEYVALUESTORE']._serialized_end=450
# @@protoc_insertion_point(module_scope)
