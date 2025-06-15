import lbclient
import utils
import time
import argparse
import random # Necesario para la carga de trabajo mixta

# Contadores globales (pueden ser re-inicializados o pasados como retorno)
# Los hacemos globales para simplicidad al acumular en benchmark,
# pero en funciones individuales se usan locales y se devuelven.
success_count = 0
failure_count = 0
failure_messages = []
lock_counts = {}

# Definición de los tamaños de valor para Benchmark 1
VALUE_SIZES = [512, 4 * 1024, 512 * 1024, 1 * 1024 * 1024, 4 * 1024 * 1024] # En bytes

def perform_bulk_write(client_instance, num_writes, value_size):
    """
    Realiza una serie de escrituras secuenciales en el servidor gRPC.
    Genera claves y valores aleatorios, e intenta establecerlos.
    Mide la latencia de cada petición y calcula estadísticas.
    
    Args:
        client_instance (lbclient.KeyValueClient): La instancia del cliente gRPC.
        num_writes (int): El número total de escrituras a realizar.
        value_size (int): El tamaño en bytes de los valores a generar.

    Returns:
        tuple: (success_count, failure_count, failure_messages, lock_counts, generated_keys, latency_metrics)
                Un resumen de la operación de escritura, incluyendo las claves generadas
                y un diccionario con la latencia (min, max, avg).
    """
    print(f"Iniciando {num_writes} escrituras de forma secuencial (tamaño: {value_size} B)...")

    local_success_count = 0
    local_failure_count = 0
    local_failure_messages = []
    local_lock_counts = {}
    generated_keys_list = [] # Para almacenar todas las claves generadas
    
    # Lista para almacenar el tiempo de cada petición
    request_latencies = []

    for i in range(num_writes):
        key = utils.generate_random_key()
        value = utils.generate_random_value(value_size)
        generated_keys_list.append(key) # Añadir la clave generada a la lista

        # Medir el tiempo de la petición
        start_time = time.time()
        status, message = client_instance.set(key, value)
        end_time = time.time()
        
        # Calcular la latencia de la petición y almacenarla
        latency = (end_time - start_time) * 1000 # Convertir a milisegundos
        request_latencies.append(latency)
        
        if status:
            local_success_count += 1
        else:
            local_failure_count += 1
            local_failure_messages.append(f"Key: {key}, Error: {message}")
            if "bloqueo en la posición" in message:
                try:
                    # Extraer la posición del mensaje de error para contar bloqueos específicos
                    pos_str = message.split('posición ')[1].split(' ')[0]
                    pos = int(pos_str)
                    local_lock_counts[pos] = local_lock_counts.get(pos, 0) + 1
                except:
                    pass # Fallback si el formato del mensaje cambia

        if (i + 1) % 100 == 0 or (i + 1) == num_writes:
            print(f"   Progreso: {i + 1}/{num_writes} escrituras completadas.")
            
    print(f"   BulkWrite completado para {value_size} B.")
    print(f"     Total exitosos: {local_success_count}")
    print(f"     Total fallidos: {local_failure_count}")
    if local_failure_count > 0:
        print("     Detalles de los fallos:")
        for msg in local_failure_messages:
            print(f"       - {msg}")
    
    if local_lock_counts:
        print("\n     Conteo de errores de 'bloqueo' por posición:")
        for pos, count in sorted(local_lock_counts.items()):
            print(f"       - Posición {pos}: {count} veces")

    # Calcular métricas de latencia
    min_latency = min(request_latencies) if request_latencies else 0
    max_latency = max(request_latencies) if request_latencies else 0
    avg_latency = sum(request_latencies) / len(request_latencies) if request_latencies else 0

    latency_metrics = {
        "min_latency_ms": min_latency,
        "max_latency_ms": max_latency,
        "avg_latency_ms": avg_latency
    }

    print("\n--- Métricas de Latencia (Escritura) ---")
    print(f"  Latencia Mínima: {min_latency:.2f} ms")
    print(f"  Latencia Máxima: {max_latency:.2f} ms")
    print(f"  Latencia Promedio: {avg_latency:.2f} ms")

    return local_success_count, local_failure_count, local_failure_messages, local_lock_counts, generated_keys_list, latency_metrics

def perform_bulk_read(client_instance, keys_to_read):
    """
    Realiza una serie de lecturas secuenciales en el servidor gRPC para una lista de claves.
    Mide la latencia de cada petición y calcula estadísticas.

    Args:
        client_instance (lbclient.KeyValueClient): La instancia del cliente gRPC.
        keys_to_read (list): Una lista de claves (strings) para leer.

    Returns:
        tuple: (success_count, failure_count, failure_messages, latency_metrics)
                Un resumen de la operación de lectura y un diccionario con la latencia (min, max, avg).
    """
    print(f"Iniciando {len(keys_to_read)} lecturas de forma secuencial...")

    local_success_count = 0
    local_failure_count = 0
    local_failure_messages = []
    request_latencies = [] # Lista para almacenar el tiempo de cada petición

    for i, key in enumerate(keys_to_read):
        start_time = time.time() # Inicia la medición de tiempo
        status, value = client_instance.get(key)
        end_time = time.time()   # Termina la medición de tiempo
        
        latency = (end_time - start_time) * 1000 # Convertir a milisegundos
        request_latencies.append(latency)
        
        if status:
            local_success_count += 1
            # Opcional: print(f"  Lectura exitosa de '{key}': {value[:50]}...")
        else:
            local_failure_count += 1
            local_failure_messages.append(f"Key: {key}, Error: {value}") # value contendrá el mensaje de error
            # Opcional: print(f"  Fallo al leer '{key}': {value}")

        if (i + 1) % 100 == 0 or (i + 1) == len(keys_to_read):
            print(f"  Progreso: {i + 1}/{len(keys_to_read)} lecturas completadas.")

    print(f"  BulkRead completado.")
    print(f"    Total exitosos: {local_success_count}")
    print(f"    Total fallidos: {local_failure_count}")
    if local_failure_count > 0:
        print("    Detalles de los fallos:")
        for msg in local_failure_messages:
            print(f"      - {msg}")
    
    # Calcular métricas de latencia
    min_latency = min(request_latencies) if request_latencies else 0
    max_latency = max(request_latencies) if request_latencies else 0
    avg_latency = sum(request_latencies) / len(request_latencies) if request_latencies else 0

    latency_metrics = {
        "min_latency_ms": min_latency,
        "max_latency_ms": max_latency,
        "avg_latency_ms": avg_latency
    }

    print("\n--- Métricas de Latencia (Lectura) ---")
    print(f"  Latencia Mínima: {min_latency:.2f} ms")
    print(f"  Latencia Máxima: {max_latency:.2f} ms")
    print(f"  Latencia Promedio: {avg_latency:.2f} ms")

    return local_success_count, local_failure_count, local_failure_messages, latency_metrics


def perform_mixed_workload(client_instance, num_operations, value_size, existing_keys):
    """
    Realiza una carga de trabajo mixta (50% lectura, 50% escritura).
    Mide la latencia de cada petición y calcula estadísticas.

    Args:
        client_instance (lbclient.KeyValueClient): La instancia del cliente gRPC.
        num_operations (int): El número total de operaciones (lecturas + escrituras) a realizar.
        value_size (int): El tamaño en bytes de los valores para las nuevas escrituras.
        existing_keys (list): Una lista de claves preexistentes para las operaciones de lectura.

    Returns:
        tuple: (success_count, failure_count, failure_messages, latency_metrics)
                Un resumen de la operación de carga mixta y un diccionario con la latencia (min, max, avg).
    """
    print(f"Iniciando {num_operations} operaciones mixtas (50% lectura, 50% escritura, tamaño: {value_size} B)...")

    local_success_count = 0
    local_failure_count = 0
    local_failure_messages = []
    request_latencies = [] # Lista para almacenar el tiempo de cada petición

    # Asegurarse de tener claves existentes para leer
    if not existing_keys:
        print("Advertencia: No hay claves existentes para operaciones de lectura en la carga de trabajo mixta.")

    for i in range(num_operations):
        start_time = time.time() # Inicia la medición de tiempo
        
        # 50% de lectura, 50% de escritura
        if random.random() < 0.5 and existing_keys: # Asegurarse de que haya claves para leer
            # Operación de lectura
            key_to_read = random.choice(existing_keys)
            status, message = client_instance.get(key_to_read)
            if status:
                local_success_count += 1
            else:
                local_failure_count += 1
                local_failure_messages.append(f"Lectura Fallida Key: {key_to_read}, Error: {message}")
        else:
            # Operación de escritura (si no hay claves existentes, por defecto será escritura)
            new_key = utils.generate_random_key()
            new_value = utils.generate_random_value(value_size)
            status, message = client_instance.set(new_key, new_value) # client.set ya tiene reintentos
            if status:
                local_success_count += 1
                # Si una nueva escritura es exitosa, agrégala a las claves existentes para futuras lecturas
                existing_keys.append(new_key) 
            else:
                local_failure_count += 1
                local_failure_messages.append(f"Escritura Fallida Key: {new_key}, Error: {message}")
        
        end_time = time.time()   # Termina la medición de tiempo
        latency = (end_time - start_time) * 1000 # Convertir a milisegundos
        request_latencies.append(latency)

        if (i + 1) % 100 == 0 or (i + 1) == num_operations:
            print(f"  Progreso: {i + 1}/{num_operations} operaciones completadas.")

    print(f"  Carga de trabajo mixta completada para {value_size} B.")
    print(f"    Total exitosos: {local_success_count}")
    print(f"    Total fallidos: {local_failure_count}")
    if local_failure_count > 0:
        print("    Detalles de los fallos:")
        for msg in local_failure_messages:
            print(f"      - {msg}")

    # Calcular métricas de latencia
    min_latency = min(request_latencies) if request_latencies else 0
    max_latency = max(request_latencies) if request_latencies else 0
    avg_latency = sum(request_latencies) / len(request_latencies) if request_latencies else 0

    latency_metrics = {
        "min_latency_ms": min_latency,
        "max_latency_ms": max_latency,
        "avg_latency_ms": avg_latency
    }

    print("\n--- Métricas de Latencia (Carga Mixta) ---")
    print(f"  Latencia Mínima: {min_latency:.2f} ms")
    print(f"  Latencia Máxima: {max_latency:.2f} ms")
    print(f"  Latencia Promedio: {avg_latency:.2f} ms")

    return local_success_count, local_failure_count, local_failure_messages, latency_metrics


def main():
    """
    Función principal para ejecutar el cliente gRPC.
    """
    parser = argparse.ArgumentParser(description='gRPC Key-Value Store Client')
    parser.add_argument('action', choices=['set', 'get', 'getPrefix', 'resetDb', 'benchmark'], help='Acción a realizar')
    parser.add_argument('--key', help='Clave para las operaciones set/get')
    parser.add_argument('--value_size', type=int, default=512, help='Tamaño del valor en bytes para la operación set (por defecto: 512)')
    parser.add_argument('--prefix', help='Prefijo para la operación getPrefix')
    parser.add_argument('--num_operations', type=int, default=1000, help='Número total de operaciones para cargas de trabajo (benchmark) (por defecto: 1000)')

    args = parser.parse_args()

    print("Iniciando el cliente...")

    client = lbclient.KeyValueClient() # Crea una única instancia del cliente

    if args.action == 'benchmark':
        print("\n--- Iniciando Benchmark 1 (Single Client) ---")
        # 2. Pre-poblar la DB con datos para la lectura
        # Usamos args.num_operations para la cantidad de escrituras iniciales
        write_start_time = time.time()
        success_w, failed_w, _, _, generated_keys, write_latency_metrics = perform_bulk_write(client, args.num_operations, args.value_size)
        write_end_time = time.time()

        # --- Fase 2: Carga de trabajo 50% lectura y 50% escritura (Mixed Workload) ---
        # print("\n  >> Ejecutando carga de trabajo 50% lectura / 50% escritura...")
        # mixed_start_time = time.time()
        # # Se usa 'generated_keys' como 'existing_keys' para la carga mixta
        # success_m, failed_m, _, mixed_latency_metrics = perform_mixed_workload(client, args.num_operations, args.value_size, generated_keys)
        # mixed_end_time = time.time()
        
        # print(f"  Carga de trabajo mixta completada. Éxitos: {success_m}, Fallos: {failed_m}. Tiempo: {mixed_end_time - mixed_start_time:.2f}s")
        # print(f"  Métricas de latencia de carga mixta: Min={mixed_latency_metrics['min_latency_ms']:.2f}ms, Max={mixed_latency_metrics['max_latency_ms']:.2f}ms, Avg={mixed_latency_metrics['avg_latency_ms']:.2f}ms")

        # print("\n--- Benchmark 1 Finalizado ---")
        # # comando para detener el cliente
        # input("Presiona Enter para continuar...")
        # print("El script ha continuado.")
        
    else:
        # Lógica para las operaciones individuales (set, get, getPrefix, resetDb)
        if args.action == 'set':
            if not args.key:
                print("Error: La clave es requerida para la operación set")
                client.close()
                return
            value = utils.generate_random_value(args.value_size)
            status, message = client.set(args.key, value)
            print(f"Operación set: Estado = {status}, Mensaje = {message}")

        elif args.action == 'get':
            if not args.key:
                print("Error: La clave es requerida para la operación get")
                client.close()
                return
            status, value = client.get(args.key)
            if status:
                print(f"Operación get: Valor = {value}")
            else:
                print(f"Operación get: Fallo = {value}")

        elif args.action == 'getPrefix':
            if not args.prefix:
                print("Error: El prefijo es requerido para la operación getPrefix")
                client.close()
                return
            status, objects_list = client.get_prefix(args.prefix)
            if status:
                print(f"Operación getPrefix: Claves encontradas = {len(objects_list)}")
                for obj in objects_list:
                    print(f"  - Clave: {obj.clave}, Valor: {obj.valor[:50]}...")
            else:
                print(f"Operación getPrefix: Fallo = {objects_list}")

        elif args.action == 'resetDb':
            status, message = client.reset_db()
            print(f"Operación resetDb: Estado = {status}, Mensaje = {message}")

    client.close() # Cierra la conexión al final de la operación
            
    print("Cliente finalizado.")

if __name__ == '__main__':
    main()
