import grpc
# Assuming your proto file is named 'bd_service.proto'
# and generates bd_service_pb2.py and bd_service_pb2_grpc.py
import conexion_pb2 as pb
import conexion_pb2_grpc as pb_grpc
import utils # Make sure 'utils' is relevant if you need it
import time

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
            response = self.stub.set(request)
            
            return response.estado, response.mensaje
        except grpc.RpcError as e:
            # print(f"Error gRPC al establecer la clave '{key}': {e}")
            return False, str(e)

    def get(self, key):
        print(f"Intentando obtener el valor para la clave: {key}")
        # Use the correct request message name: Consultar
        retries = 0
        while retries < max_retries:
            request = pb.Insertar(clave=key, valor=value)
            try:
                response = self.stub.Set(request) # Método Set (PascalCase)
                if response.estado:
                    # print(f"Respuesta del servidor para '{key}': Estado = {response.estado}, Mensaje = {response.mensaje}")
                    return response.estado, response.mensaje
                else:
                    # Si el estado es False, verificamos si es un error de "bloqueo"
                    if "bloqueo en la posición" in response.mensaje:
                        retries += 1
                        delay = (base_delay_ms / 1000.0) * (2 ** (retries - 1)) # Backoff exponencial
                        print(f"Reintentando '{key}' ({retries}/{max_retries} intentos) debido a '{response.mensaje}'. Esperando {delay:.2f}s...")
                        time.sleep(delay)
                    else:
                        # Si no es un error de bloqueo, devolver el error inmediatamente
                        print(f"Fallo al establecer la clave '{key}' (no es bloqueo): {response.mensaje}")
                        return response.estado, response.mensaje
            except grpc.RpcError as e:
                # Errores de comunicación gRPC (e.g., servidor no disponible)
                print(f"Error gRPC al establecer la clave '{key}': {e}")
                return False, str(e)
            except Exception as e:
                # Otros errores inesperados
                print(f"Error inesperado al establecer la clave '{key}': {e}")
                return False, str(e)
        
        # Si se agotan los reintentos
        return False, f"Fallo al establecer la clave '{key}' después de {max_retries} reintentos. Último mensaje: {message}"


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

    def reset_db(self):
        print("Intentando resetear la base de datos")
        request = pb.RequestResetDb()
        try:
            # Call the correct method name: GetPrefix
            response = self.stub.resetDb(request)
            print(f"Respuesta del servidor: Estado = {response.estado}, Mensaje = {response.mensaje}")
            return response.estado, response.mensaje # Return state and list of objects
        except grpc.RpcError as e:
            print(f"Error gRPC al resetear la base de datos: {e}")
            return False, str(e)

    def close(self):
        print("Cerrando la conexión con el servidor.")
        self.channel.close()
        print("Conexión cerrada.")