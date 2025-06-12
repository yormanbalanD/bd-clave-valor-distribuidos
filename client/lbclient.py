import grpc
# Assuming your proto file is named 'bd_service.proto'
# and generates bd_service_pb2.py and bd_service_pb2_grpc.py
import conexion_pb2 as pb
import conexion_pb2_grpc as pb_grpc
import utils # Make sure 'utils' is relevant if you need it

class KeyValueClient:
    def __init__(self, server_address='localhost:5050'): # Ensure this matches your Go server's port (50051 based on your main.go)
        print(f"Conectando al servidor en: {server_address}")
        self.channel = grpc.insecure_channel(server_address)
        # Use the correct service stub name: BDStub
        self.stub = pb_grpc.BDStub(self.channel)
        print("Cliente gRPC inicializado.")

    def set(self, key, value):
        print(f"Intentando establecer la clave: {key}")
        # Use the correct request message name: Insertar
        request = pb.Insertar(clave=key, valor=value)
        try:
            # Call the correct method name: Set (PascalCase as per gRPC convention)
            response = self.stub.set(request)
            print(f"Respuesta del servidor: Status = {response.estado}, Message = {response.mensaje}") # Update field names
            return response.estado, response.mensaje
        except grpc.RpcError as e:
            print(f"Error gRPC al establecer la clave: {e}")
            return False, str(e)

    def get(self, key):
        print(f"Intentando obtener el valor para la clave: {key}")
        # Use the correct request message name: Consultar
        request = pb.Consultar(clave=key)
        try:
            # Call the correct method name: Get
            response = self.stub.get(request)
            # Update field names based on your RespuestaGet message
            print(f"Respuesta del servidor: Estado = {response.estado}, Mensaje = {response.mensaje}")
            # You might return response.objeto.valor if it exists
            return response.estado, response.objeto.valor if response.estado else response.mensaje
        except grpc.RpcError as e:
            print(f"Error gRPC al obtener la clave: {e}")
            return False, str(e)

    def get_prefix(self, prefix):
        print(f"Intentando obtener valores con el prefijo: {prefix}")
        # Use the correct request message name: Consultar for GetPrefix (if that's what your proto means)
        # Your proto has: rpc getPrefix (Consultar)
        request = pb.Consultar(clave=prefix) # Assuming 'clave' is used for prefix
        try:
            # Call the correct method name: GetPrefix
            response = self.stub.getPrefix(request)
            print(f"Respuesta del servidor: Estado = {response.estado}, Mensaje = {response.mensaje}, Objetos = {response.objetos}")
            return response.estado, response.objetos # Return state and list of objects
        except grpc.RpcError as e:
            print(f"Error gRPC al obtener el prefijo: {e}")
            return False, str(e)

    def close(self):
        print("Cerrando la conexión con el servidor.")
        self.channel.close()
        print("Conexión cerrada.")