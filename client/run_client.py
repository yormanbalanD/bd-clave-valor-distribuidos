import lbclient
import utils
import time
import argparse
from concurrent.futures import ThreadPoolExecutor

# Contador global para seguir los éxitos y fallos
success_count = 0
failure_count = 0
failure_messages = []
lock_counts = {} # Para contar cuántas veces se produce un bloqueo en cada posición

def bulk_write_task(client_instance, num_writes, value_size):
    """
    Tarea individual para un hilo en bulk_write, maneja su propia cuenta de éxitos/fallos.
    """
    local_success = 0
    local_failure = 0
    local_failure_messages = []

    for _ in range(num_writes):
        key = utils.generate_random_key()
        value = utils.generate_random_value(value_size)
        status, message = client_instance.set(key, value) # client.set ahora tiene reintentos
        
        if status:
            local_success += 1
        else:
            local_failure += 1
            local_failure_messages.append(f"Key: {key}, Error: {message}")
            if "bloqueo en la posición" in message:
                try:
                    pos_str = message.split('posición ')[1].split(' ')[0]
                    pos = int(pos_str)
                    if pos not in lock_counts:
                        lock_counts[pos] = 0
                    lock_counts[pos] += 1
                except:
                    pass # Fallback if message format changes
    
    return local_success, local_failure, local_failure_messages

def main():
    """
    Función principal para ejecutar el cliente gRPC.
    """
    parser = argparse.ArgumentParser(description='gRPC Key-Value Store Client')
    parser.add_argument('action', choices=['set', 'get', 'getPrefix', 'bulkWrite', 'resetDb'], help='Acción a realizar')
    parser.add_argument('--key', help='Clave para las operaciones set/get')
    parser.add_argument('--value_size', type=int, default=512, help='Tamaño del valor en bytes para la operación set (por defecto: 512)')
    parser.add_argument('--prefix', help='Prefijo para la operación getPrefix')
    parser.add_argument('--num_writes', type=int, default=10, help='Número de escrituras para la operación bulkWrite (por defecto: 10)')
    parser.add_argument('--num_clients', type=int, choices=[2, 4, 8, 16, 32], default=2, help='Número de clientes concurrentes (2, 4, 8, 16, 32)')

    args = parser.parse_args()

    print("Iniciando el cliente...")  # Imprime un mensaje de inicio

    if args.action == 'bulkWrite':
        start_time = time.time()
        print(f"Iniciando bulkWrite con {args.num_writes} escrituras usando {args.num_clients} clientes...")

        # Divide el número total de escrituras entre el número de clientes
        writes_per_client = args.num_writes // args.num_clients
        # Si el número de escrituras no es divisible, el primer cliente hará las restantes
        remainder_writes = args.num_writes % args.num_clients

        with ThreadPoolExecutor(max_workers=args.num_clients) as executor:
            clients = [lbclient.KeyValueClient() for _ in range(args.num_clients)]
            futures = []
            for i, client in enumerate(clients):
                current_writes = writes_per_client
                if i == 0: # El primer cliente maneja el resto si no es divisible
                    current_writes += remainder_writes
                futures.append(executor.submit(bulk_write_task, client, current_writes, args.value_size))

            # Recopilar los resultados de todas las tareas
            global success_count, failure_count, failure_messages, lock_counts # Asegurarse de usar las variables globales
            for future in futures:
                local_s, local_f, local_msgs = future.result()
                success_count += local_s
                failure_count += local_f
                failure_messages.extend(local_msgs)

        end_time = time.time()
        print(f"BulkWrite completado en {end_time - start_time:.2f} segundos")
        print(f"  Total exitosos: {success_count}")
        print(f"  Total fallidos: {failure_count}")
        if failure_count > 0:
            print("  Detalles de los fallos:")
            for msg in failure_messages:
                print(f"    - {msg}")


        # Cerrar todos los clientes
        for client in clients:
            client.close()

    else:
        client = lbclient.KeyValueClient() # Crea una instancia del cliente

        if args.action == 'set':
            if not args.key:
                print("Error: La clave es requerida para la operación set")
                return
            value = utils.generate_random_value(args.value_size)
            status, message = client.set(args.key, value)
            print(f"Operación set: Estado = {status}, Mensaje = {message}")

        elif args.action == 'get':
            if not args.key:
                print("Error: La clave es requerida para la operación get")
                return
            status, value = client.get(args.key) # Obtén el estado y el valor/mensaje
            if status:
                print(f"Operación get: Valor = {value}")
            else:
                print(f"Operación get: Fallo = {value}") # Imprime el mensaje de error

        elif args.action == 'getPrefix':
            if not args.prefix:
                print("Error: El prefijo es requerido para la operación getPrefix")
                return
            status, objects_list = client.get_prefix(args.prefix) # Obtén el estado y la lista de objetos
            if status:
                print(f"Operación getPrefix: Claves encontradas = {len(objects_list)}")
                for obj in objects_list:
                    print(f"  - Clave: {obj.clave}, Valor: {obj.valor[:50]}...") # Limita la longitud del valor para imprimir
            else:
                print(f"Operación getPrefix: Fallo = {objects_list}") # Imprime el mensaje de error

        elif args.action == 'resetDb':
            status, message = client.reset_db()
            print(f"Operación resetDb: Estado = {status}, Mensaje = {message}")

        client.close()  # Cierra la conexión al final
        

    print("Cliente finalizado.")  # Imprime un mensaje de finalización

if __name__ == '__main__':
    main()
