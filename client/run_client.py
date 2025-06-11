import lbclient
import utils
import time
import argparse

def main():
    """
    Función principal para ejecutar el cliente gRPC.
    """
    parser = argparse.ArgumentParser(description='gRPC Key-Value Store Client')
    parser.add_argument('action', choices=['set', 'get', 'getPrefix', 'bulkWrite'], help='Acción a realizar')
    parser.add_argument('--key', help='Clave para las operaciones set/get')
    parser.add_argument('--value_size', type=int, default=512, help='Tamaño del valor en bytes para la operación set (por defecto: 512)')
    parser.add_argument('--prefix', help='Prefijo para la operación getPrefix')
    parser.add_argument('--num_writes', type=int, default=10, help='Número de escrituras para la operación bulkWrite (por defecto: 10)')

    args = parser.parse_args()

    print("Iniciando el cliente...")  # Imprime un mensaje de inicio
    client = lbclient.KeyValueClient()  # Crea una instancia del cliente

    if args.action == 'set':
        if not args.key:
            print("Error: La clave es requerida para la operación set")
            return
        value = utils.generate_random_value(args.value_size)
        status, message = client.set(args.key, value)
        print(f"Operación set: Status = {status}, Message = {message}")

    elif args.action == 'get':
        if not args.key:
            print("Error: La clave es requerida para la operación get")
            return
        found, value = client.get(args.key)
        if found:
            print(f"Operación get: Value = {value}")
        else:
            print("Clave no encontrada")

    elif args.action == 'getPrefix':
        if not args.prefix:
            print("Error: El prefijo es requerido para la operación getPrefix")
            return
        values = client.get_prefix(args.prefix)
        if values:
            print(f"Operación getPrefix: Values = {values}")
        else:
            print("No se encontraron claves con ese prefijo")

    elif args.action == 'bulkWrite':
        start_time = time.time()
        print(f"Iniciando bulkWrite con {args.num_writes} escrituras...")  # Imprime el número de escrituras
        for i in range(args.num_writes):
            key = utils.generate_random_key()
            value = utils.generate_random_value(args.value_size)
            status, message = client.set(key, value)
            if not status:
                print(f"Error durante bulkWrite: {message}")
                break
        end_time = time.time()
        print(f"BulkWrite completado en {end_time - start_time:.2f} segundos")

    client.close()  # Cierra la conexión al final
    print("Cliente finalizado.")  # Imprime un mensaje de finalización

if __name__ == '__main__':
    main()
