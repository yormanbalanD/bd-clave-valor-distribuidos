import grpc
import client_pb2
import client_pb2_grpc
import utils

class KeyValueClient:
    def __init__(self, server_address='localhost:5050'):
        """
        Inicializa el cliente gRPC.

        Args:
            server_address (str): La dirección del servidor gRPC.
        """
        print(f"Conectando al servidor en: {server_address}")  # Imprime la dirección del servidor
        self.channel = grpc.insecure_channel(server_address)  # Crea un canal de comunicación inseguro
        self.stub = client_pb2_grpc.KeyValueStoreStub(self.channel)  # Crea un stub (cliente) para el servicio
        print("Cliente gRPC inicializado.")  # Imprime un mensaje de confirmación

    def set(self, key, value):
        """
        Realiza una llamada RPC al servidor para establecer un valor para una clave.

        Args:
            key (str): La clave a establecer.
            value (bytes): El valor a asociar con la clave.

        Returns:
            tuple: Una tupla que contiene el estado (True/False) y un mensaje.
        """
        print(f"Intentando establecer la clave: {key}")  # Imprime la clave que se va a establecer
        request = client_pb2.SetRequest(key=key, value=value)  # Crea una solicitud SetRequest
        try:
            response = self.stub.Set(request)  # Llama al método Set en el servidor
            print(f"Respuesta del servidor: Status = {response.status}, Message = {response.message}")  # Imprime la respuesta del servidor
            return response.status, response.message  # Devuelve el estado y el mensaje
        except grpc.RpcError as e:
            print(f"Error gRPC al establecer la clave: {e}")  # Imprime el error gRPC
            return False, str(e)  # Devuelve False y el mensaje de error

    def get(self, key):
        """
        Realiza una llamada RPC al servidor para obtener el valor asociado con una clave.

        Args:
            key (str): La clave a buscar.

        Returns:
            tuple: Una tupla que contiene un booleano (True si se encontró la clave, False si no) y el valor (si se encontró).
        """
        print(f"Intentando obtener el valor para la clave: {key}")  # Imprime la clave que se va a buscar
        request = client_pb2.GetRequest(key=key)  # Crea una solicitud GetRequest
        try:
            response = self.stub.Get(request)  # Llama al método Get en el servidor
            print(f"Respuesta del servidor: Found = {response.found}")  # Imprime si la clave fue encontrada
            return response.found, response.value  # Devuelve True/False y el valor
        except grpc.RpcError as e:
            print(f"Error gRPC al obtener la clave: {e}")  # Imprime el error gRPC
            return False, str(e)  # Devuelve False y el mensaje de error

    def get_prefix(self, prefix):
        """
        Realiza una llamada RPC al servidor para obtener una lista de valores cuyas claves comienzan con un prefijo.

        Args:
            prefix (str): El prefijo a buscar.

        Returns:
            list: Una lista de valores que coinciden con el prefijo.
        """
        print(f"Intentando obtener valores con el prefijo: {prefix}")  # Imprime el prefijo que se va a buscar
        request = client_pb2.GetPrefixRequest(prefix=prefix)  # Crea una solicitud GetPrefixRequest
        try:
            response = self.stub.GetPrefix(request)  # Llama al método GetPrefix en el servidor
            print(f"Respuesta del servidor: Values = {response.values}")  # Imprime los valores encontrados
            return response.values  # Devuelve la lista de valores
        except grpc.RpcError as e:
            print(f"Error gRPC al obtener el prefijo: {e}")  # Imprime el error gRPC
            return None, str(e)  # Devuelve None y el mensaje de error

    def close(self):
        """
        Cierra el canal de comunicación con el servidor.
        """
        print("Cerrando la conexión con el servidor.")  # Imprime un mensaje de cierre
        self.channel.close()  # Cierra el canal
        print("Conexión cerrada.")  # Imprime un mensaje de confirmación
