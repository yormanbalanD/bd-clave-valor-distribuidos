import os
import random

def generate_random_key(length=16):
    """
    Genera una clave aleatoria de la longitud especificada.

    Args:
        length (int): La longitud de la clave en bytes.

    Returns:
        str: Una cadena hexadecimal que representa la clave.
    """
    key = os.urandom(length).hex()  # Genera bytes aleatorios y los convierte a hexadecimal
    print(f"Clave generada: {key}")  # Imprime la clave generada
    return key

def generate_random_value(size_bytes):
    """
    Genera un valor aleatorio del tamaño especificado.

    Args:
        size_bytes (int): El tamaño del valor en bytes.

    Returns:
        bytes: Una cadena de bytes aleatorios.
    """
    value = os.urandom(size_bytes)  # Genera bytes aleatorios
    print(f"Valor generado de tamaño: {size_bytes} bytes")  # Imprime el tamaño del valor generado
    return value
