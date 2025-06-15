import grpc
# Assuming your proto file is named 'bd_service.proto'
# and generates bd_service_pb2.py and bd_service_pb2_grpc.py
import conexion_pb2 as pb
import conexion_pb2_grpc as pb_grpc
import utils # Make sure 'utils' is relevant if you need it
import time

max_retries = 3

class KeyValueClient:
    def __init__(self, server_address='localhost:5050'): # Ensure this matches your Go server's port (50051 based on your main.go)
        
        options = [
            ('grpc.max_send_message_length', 1024*1024*1024),
            ('grpc.max_receive_message_length', 1024*1024*1024),
        ]
        print(f"Conectando al servidor en: {server_address}")
        self.channel = grpc.insecure_channel(server_address, options=options)
        # Use the correct service stub name: BDStub
        self.stub = pb_grpc.BDStub(self.channel)
        print("Cliente gRPC inicializado.")

    def set(self, key, value, max_retries=5, base_delay_ms=20):
        """
        Intenta establecer una clave-valor, con reintentos para errores de bloqueo.

        Args:
            key (str): La clave a establecer.
            value (str): El valor a asociar con la clave.
            max_retries (int): Número máximo de reintentos en caso de errores de bloqueo.
            base_delay_ms (int): Retraso base en milisegundos para el backoff exponencial.

        Returns:
            tuple: (estado_exitoso, mensaje_o_valor)
        """
        # print(f"Intentando establecer la clave: {key}") # Comentado para reducir la salida en bulkWrite
        retries = 0
        message = "" # Inicializar message para el caso de no entrar al bucle o fallar en el primer intento
        while retries < max_retries:
            request = pb.Insertar(clave=key, valor=value)
            try:
                response = self.stub.set(request) # Método Set (PascalCase)
                if response.estado:
                    # print(f"Respuesta del servidor para '{key}': Estado = {response.estado}, Mensaje = {response.mensaje}")
                    return response.estado, response.mensaje
                else:
                    message = response.mensaje # Actualizar el mensaje de error
                    # Si el estado es False, verificamos si es un error de "bloqueo"
                    if "bloqueo en la posición" in response.mensaje:
                        retries += 1
                        delay = (base_delay_ms / 1000.0) * (2 ** (retries - 1)) # Backoff exponencial
                        # print(f"Reintentando '{key}' ({retries}/{max_retries} intentos) debido a '{response.mensaje}'. Esperando {delay:.2f}s...")
                        time.sleep(delay)
                    else:
                        # Si no es un error de bloqueo, devolver el error inmediatamente
                        # print(f"Fallo al establecer la clave '{key}' (no es bloqueo): {response.mensaje}")
                        return response.estado, response.mensaje
            except grpc.RpcError as e:
                # Errores de comunicación gRPC (e.g., servidor no disponible)
                message = str(e) # Capturar el mensaje de error gRPC
                # print(f"Error gRPC al establecer la clave '{key}': {e}")
                return False, str(e)
            except Exception as e:
                # Otros errores inesperados
                message = str(e) # Capturar el mensaje de error
                # print(f"Error inesperado al establecer la clave '{key}': {e}")
                return False, str(e)
        
        # Si se agotan los reintentos
        return False, f"Fallo al establecer la clave '{key}' después de {max_retries} reintentos. Último mensaje: {message}"


    def get(self, key):
        request = pb.Consultar(clave=key)
        try:
            response = self.stub.get(request) # Método Get (PascalCase)
            # print(f"Respuesta del servidor: Estado = {response.estado}, Mensaje = {response.mensaje}")
            # Si el estado es True, devolver el valor del objeto. Si es False, devolver el mensaje de error.
            return response.estado, response.objeto.valor if response.estado else response.mensaje
        except grpc.RpcError as e:
            # print(f"Error gRPC al obtener la clave: {e}")
            return False, str(e)
        except Exception as e:
            # print(f"Error inesperado al obtener la clave: {e}")
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