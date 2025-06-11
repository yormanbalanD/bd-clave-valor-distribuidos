# Generado por el complemento del compilador del protocolo Python gRPC. ¡NO EDITAR!
"""Clases de cliente y servidor correspondientes a los servicios definidos por protobuf."""
import grpc
import warnings

import client_pb2 as client__pb2

GRPC_GENERATED_VERSION = '1.73.0'
GRPC_VERSION = grpc.__version__
_version_not_supported = False

try:
    from grpc._utilities import first_version_is_lower
    _version_not_supported = first_version_is_lower(GRPC_VERSION, GRPC_GENERATED_VERSION)
except ImportError:
    _version_not_supported = True

if _version_not_supported:
    raise RuntimeError(
        f' El paquete grpc instalado está en la versión {GRPC_VERSION},'
        + f' pero el código generado en client_pb2_grpc.py depende de'
        + f' grpcio>={GRPC_GENERATED_VERSION}.'
        + f' Actualice su módulo grpc a grpcio>={GRPC_GENERATED_VERSION}'
        + f' o degradar el código generado usando grpcio-tools<={GRPC_VERSION}.'
    )


class KeyValueStoreStub(object):
    """Falta el comentario de la documentación asociada en el archivo .proto."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.Set = channel.unary_unary(
                '/kvstore.KeyValueStore/Set',
                request_serializer=client__pb2.SetRequest.SerializeToString,
                response_deserializer=client__pb2.SetResponse.FromString,
                _registered_method=True)
        self.Get = channel.unary_unary(
                '/kvstore.KeyValueStore/Get',
                request_serializer=client__pb2.GetRequest.SerializeToString,
                response_deserializer=client__pb2.GetResponse.FromString,
                _registered_method=True)
        self.GetPrefix = channel.unary_unary(
                '/kvstore.KeyValueStore/GetPrefix',
                request_serializer=client__pb2.GetPrefixRequest.SerializeToString,
                response_deserializer=client__pb2.GetPrefixResponse.FromString,
                _registered_method=True)


class KeyValueStoreServicer(object):
    """Falta el comentario de la documentación asociada en el archivo .proto."""

    def Set(self, request, context):
        """Falta el comentario de la documentación asociada en el archivo .proto."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('¡Metodo no implementado!!')
        raise NotImplementedError('¡Metodo no implementado!')

    def Get(self, request, context):
        """Falta el comentario de la documentación asociada en el archivo .proto"""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('¡Metodo no implementado!')
        raise NotImplementedError('¡Metodo no implementado!')

    def GetPrefix(self, request, context):
        """Falta el comentario de la documentación asociada en el archivo .proto."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('¡Metodo no implementado!')
        raise NotImplementedError('¡Metodo no implementado!')


def add_KeyValueStoreServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'Set': grpc.unary_unary_rpc_method_handler(
                    servicer.Set,
                    request_deserializer=client__pb2.SetRequest.FromString,
                    response_serializer=client__pb2.SetResponse.SerializeToString,
            ),
            'Get': grpc.unary_unary_rpc_method_handler(
                    servicer.Get,
                    request_deserializer=client__pb2.GetRequest.FromString,
                    response_serializer=client__pb2.GetResponse.SerializeToString,
            ),
            'GetPrefix': grpc.unary_unary_rpc_method_handler(
                    servicer.GetPrefix,
                    request_deserializer=client__pb2.GetPrefixRequest.FromString,
                    response_serializer=client__pb2.GetPrefixResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'kvstore.KeyValueStore', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))
    server.add_registered_method_handlers('kvstore.KeyValueStore', rpc_method_handlers)


 # Esta clase es parte de una API EXPERIMENTAL.
class KeyValueStore(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def Set(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/kvstore.KeyValueStore/Set',
            client__pb2.SetRequest.SerializeToString,
            client__pb2.SetResponse.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)

    @staticmethod
    def Get(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/kvstore.KeyValueStore/Get',
            client__pb2.GetRequest.SerializeToString,
            client__pb2.GetResponse.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)

    @staticmethod
    def GetPrefix(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/kvstore.KeyValueStore/GetPrefix',
            client__pb2.GetPrefixRequest.SerializeToString,
            client__pb2.GetPrefixResponse.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)
