package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	pb "github.com/yormanbalanD/bd-clave-valor-distribuidos/proto"
	"google.golang.org/grpc"
)

var (
	port = flag.Int("port", 5050, "The server port")
)

const (
	B512  = 512
	KB4   = 1024 * 4
	KB512 = 1024 * 512
	MB1   = 1024 * 1024
	MB4   = 1024 * 1024 * 4
)

type InfClave struct {
	Clave     [16]byte
	Tamaño    int32
	Direccion int64
}

var InfClaveSize = 16 + 4 + 8

type DatosDiccionario struct {
	Clave         string
	Valor         string
	PosicionValue int64
	PosicionKey   int64
	Tamaño        int32
}

const (
	WHERE_FILESYSTEM = 0
	WHERE_HAST_TABLE = 1
)

var tablaHash = make(map[string]DatosDiccionario)
var tablaHashMutex sync.RWMutex

var bloqueoValues = make(map[int64]string)
var bloqueoValuesMutex sync.Mutex

var bloqueoKeys = make(map[int64]string)
var bloqueoKeysMutex sync.Mutex

func getValue(pos int64, tamaño int32) (string, error) {
	var fileValues, err = os.OpenFile("./db/values.db", os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		fmt.Println("Error al abrir/crear el archivo Values de la DB:", err)
		return "", errors.New("error al abrir/crear el archivo Values de la DB")
	}
	defer fileValues.Close()

	var buf = make([]byte, tamaño)
	_, err = fileValues.Seek(pos, io.SeekStart) // Use _ for ignored return value
	if err != nil {
		fmt.Println("Error al posicionar el archivo Values para lectura:", err)
		return "", errors.New("error al posicionar el archivo Values para lectura")
	}

	n, err := fileValues.Read(buf)
	if err != nil {
		// Specific check for EOF when reading less than expected.
		// If n < tamaño AND err == io.EOF, it means the record is incomplete.
		if err == io.EOF && n < int(tamaño) {
			fmt.Printf("Error al leer el archivo Values: EOF inesperado, se leyeron %d de %d bytes\n", n, tamaño)
			return "", fmt.Errorf("error al leer el archivo Values: registro incompleto en pos %d", pos)
		}
		fmt.Println("Error al leer el archivo Values:", err)
		return "", errors.New("error al leer el archivo Values")
	}
	if n < int(tamaño) {
		// This case means Read didn't return EOF but also didn't fill the buffer.
		// This should theoretically not happen with os.File.Read on valid files,
		// but it's good for robustness.
		fmt.Printf("Advertencia: Se leyeron menos bytes de lo esperado (%d de %d) del archivo Values en pos %d\n", n, tamaño, pos)
	}

	return string(bytes.TrimRight(buf, "\x00")), nil
}

func searchKey(key string, where int8) (string, error) {
	if where == WHERE_FILESYSTEM {
		var fileKeys, err = os.OpenFile("./db/keys.db", os.O_RDWR|os.O_CREATE, 0644)

		if err != nil {
			fmt.Println("Error al abrir/crear el archivo Keys de la DB:", err)
			return "", errors.New("error al abrir/crear el archivo Keys de la DB")
		}
		defer fileKeys.Close()

		var buf = make([]byte, InfClaveSize)
		_, err = fileKeys.Seek(0, io.SeekStart)
		if err != nil {
			fmt.Println("Error al posicionar el archivo Keys:", err)
			return "", errors.New("error al posicionar el archivo Keys")
		}

		var clave InfClave
		claveEncontrada := false

		for {
			n, err := fileKeys.Read(buf)
			if err != nil {
				if err == io.EOF {
					// fmt.Println("Se ha leido el último elemento del archivo Keys")
					break // End of file
				}
				// If n < InfClaveSize and err is not EOF, it might be a partial record due to corruption
				if n > 0 && n < InfClaveSize {
					fmt.Printf("Error al leer el archivo Keys: registro incompleto, se leyeron %d de %d bytes\n", n, InfClaveSize)
					return "", fmt.Errorf("error al leer el archivo Keys: registro incompleto")
				}
				fmt.Println("Error al leer el archivo Keys:", err)
				return "", errors.New("error al leer el archivo Keys")
			}
			if n < InfClaveSize { // Should not happen unless EOF is also returned
				fmt.Printf("Advertencia: Se leyeron menos bytes de lo esperado (%d de %d) del archivo Keys\n", n, InfClaveSize)
				continue // Skip this potentially corrupted record
			}

			var temp InfClave
			reader := bytes.NewReader(buf)
			err = binary.Read(reader, binary.LittleEndian, &temp)
			if err != nil {
				fmt.Println("Error al deserializar InfClave:", err)
				return "", errors.New("error al deserializar InfClave")
			}

			claveString := strings.TrimRight(string(temp.Clave[:16]), "\x00")
			if claveString == key {
				claveEncontrada = true
				clave = temp
				break
			}
		}

		if !claveEncontrada {
			return "", errors.New("clave no encontrada")
		}

		valor, err := getValue(clave.Direccion, clave.Tamaño)

		if err != nil {
			fmt.Println("Error al obtener valor desde values.db:", err)
			return "", errors.New("error al obtener valor desde values.db")
		}

		return valor, nil
	}

	if where == WHERE_HAST_TABLE {
		tablaHashMutex.RLock()
		// Ensure the lock is released when the function exits (or when read is done)
		defer tablaHashMutex.RUnlock()

		value, exist := tablaHash[key]
		if !exist {
			return "", errors.New("clave no encontrada")
		}

		return value.Valor, nil
	}

	return "", errors.New("error al leer el archivo")
}

func searchKeyPrefix(key string, where int8) ([]*pb.Objeto, error) {
	if where == WHERE_FILESYSTEM {
		var fileKeys, err = os.OpenFile("./db/keys.db", os.O_RDWR|os.O_CREATE, 0644)

		if err != nil {
			fmt.Println("Error al abrir/crear el archivo Keys de la DB:", err)
			return []*pb.Objeto{}, errors.New("error al abrir/crear el archivo Keys de la DB")
		}
		defer fileKeys.Close()

		var buf = make([]byte, InfClaveSize)
		_, err = fileKeys.Seek(0, io.SeekStart)
		if err != nil {
			fmt.Println("Error al posicionar el archivo Keys:", err)
			return []*pb.Objeto{}, errors.New("error al posicionar el archivo Keys")
		}

		var claves []InfClave

		for {
			n, err := fileKeys.Read(buf)
			if err != nil {
				if err == io.EOF {
					// fmt.Println("Se ha leido el último elemento del archivo Keys")
					break
				}
				if n > 0 && n < InfClaveSize {
					fmt.Printf("Error al leer el archivo Keys: registro incompleto, se leyeron %d de %d bytes\n", n, InfClaveSize)
					// Decide how to handle partial records. For prefix search, we might skip.
					continue
				}
				fmt.Println("Error al leer el archivo Keys:", err)
				return []*pb.Objeto{}, errors.New("error al leer el archivo Keys")
			}
			if n < InfClaveSize { // Should not happen unless EOF is also returned
				fmt.Printf("Advertencia: Se leyeron menos bytes de lo esperado (%d de %d) del archivo Keys\n", n, InfClaveSize)
				continue // Skip this potentially corrupted record
			}

			var temp InfClave
			reader := bytes.NewReader(buf)
			err = binary.Read(reader, binary.LittleEndian, &temp)
			if err != nil {
				fmt.Println("Error al deserializar InfClave:", err)
				continue // Skip this record if it's malformed
			}

			claveString := strings.TrimRight(string(temp.Clave[:16]), "\x00")

			if strings.HasPrefix(claveString, key) {
				claves = append(claves, temp)
			}
		}

		var objetos []*pb.Objeto

		for _, clave := range claves {
			valor, err := getValue(clave.Direccion, clave.Tamaño)

			if err != nil {
				fmt.Println("Error al leer el archivo:", err)
				return []*pb.Objeto{}, errors.New("error al leer el archivo")
			}

			objeto := &pb.Objeto{Clave: string(clave.Clave[:16]), Valor: valor}

			objetos = append(objetos, objeto)
		}

		return objetos, nil
	}
	if where == WHERE_HAST_TABLE {
		var objetos []*pb.Objeto

		tablaHashMutex.RLock()
		// Ensure the lock is released when the function exits (or when read is done)
		defer tablaHashMutex.RUnlock()

		for _, clave := range tablaHash {
			if strings.HasPrefix(clave.Clave, key) {
				objeto := &pb.Objeto{Clave: clave.Clave, Valor: clave.Valor}

				objetos = append(objetos, objeto)
			}

		}

		return objetos, nil
	}

	return nil, errors.New("error al leer el archivo")
}

func writeKeys(key string, posicion int64, tamaño int32) (int64, error) {
	var fileKeys, err = os.OpenFile("./db/keys.db", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		fmt.Println("Error al abrir/crear el archivo Keys de la DB:", err)
		return -1, err
	}
	defer fileKeys.Close() // Asegura que el archivo se cierre

	var pos int64

	bloqueoKeysMutex.Lock() // Protect access to `bloqueoKeys` map
	// Critical section: determine position and mark it
	tablaHashMutex.RLock() // Read lock to check if key exists in hash table
	tableValue, exist := tablaHash[key]
	tablaHashMutex.RUnlock()

	if exist {
		pos = tableValue.PosicionKey // Key exists, overwrite at its original position
	} else {
		pos, err = fileKeys.Seek(0, io.SeekEnd) // New key, append to end
		if err != nil {
			bloqueoKeysMutex.Unlock() // IMPORTANT: Unlock before returning
			fmt.Println("Error al posicionar el archivo Keys al final:", err)
			return -1, errors.New("error al posicionar el archivo Keys al final")
		}
	}

	if _, isBlocked := bloqueoKeys[pos]; isBlocked {
		bloqueoKeysMutex.Unlock() // IMPORTANT: Unlock `bloqueoKeysMutex` before returning this error
		return -1, fmt.Errorf("bloqueo en la posición %d para clave '%s'", pos, key)
	}
	bloqueoKeys[pos] = key // Mark this position as being handled
	bloqueoKeysMutex.Unlock()

	defer func(p int64) {
		bloqueoKeysMutex.Lock() // Re-acquire lock to modify the map
		delete(bloqueoKeys, p)
		bloqueoKeysMutex.Unlock()
		// println("bloqueoKeysMutex - Position", p, "unblocked.")
	}(pos)

	_, err = fileKeys.Seek(pos, io.SeekStart)
	if err != nil {
		fmt.Println("Error al posicionar el archivo Keys para escritura:", err)
		return -1, errors.New("error al posicionar el archivo Keys para escritura")
	}

	var buf bytes.Buffer
	var temp InfClave

	copy(temp.Clave[:], []byte(key))
	temp.Direccion = posicion
	temp.Tamaño = tamaño

	err = binary.Write(&buf, binary.LittleEndian, temp)
	if err != nil {
		fmt.Println("Error al serializar InfClave en buffer:", err)
		return -1, err
	}

	_, err = fileKeys.Write(buf.Bytes())
	if err != nil {
		fmt.Println("Error al escribir en el archivo keys.db:", err)
		return -1, errors.New("error al escribir en el archivo keys.db")
	}

	// fmt.Printf("Se escribieron %d bytes en keys.db en la posición %d.\n", n, pos)
	return pos, nil
}

func writeValues(key string, value string) error {
	var fileValues, err = os.OpenFile("./db/values.db", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		fmt.Println("Error al abrir/crear el archivo Values de la DB:", err)
		return errors.New("error al abrir/crear el archivo Values de la DB")
	}
	defer fileValues.Close() // Asegura que el archivo se cierre

	var tamaño int32
	var lenValue = len(value)

	if lenValue <= B512 {
		tamaño = B512
	} else if lenValue <= KB4 {
		tamaño = KB4
	} else if lenValue <= KB512 {
		tamaño = KB512
	} else if lenValue <= MB1 {
		tamaño = MB1
	} else if lenValue <= MB4 {
		tamaño = MB4
	} else {
		fmt.Println("El tamaño de la cadena es mayor a 4 MB")
		return errors.New("el tamaño de la cadena es mayor a 4 MB")
	}

	var pos int64

	bloqueoValuesMutex.Lock() // Protect access to `bloqueoValues` map
	// Critical section: determine position and mark it
	tablaHashMutex.RLock() // Read lock to check if key exists in hash table
	existingEntry, exist := tablaHash[key]
	tablaHashMutex.RUnlock()

	if exist {
		pos = existingEntry.PosicionValue // Key exists, overwrite at its original position
	} else {
		pos, err = fileValues.Seek(0, io.SeekEnd) // New value, append to end
		if err != nil {
			bloqueoValuesMutex.Unlock() // IMPORTANT: Unlock before returning
			fmt.Println("Error al posicionar el archivo Values al final:", err)
			return errors.New("error al posicionar el archivo Values al final")
		}
	}

	if _, isBlocked := bloqueoValues[pos]; isBlocked {
		bloqueoValuesMutex.Unlock() // IMPORTANT: Unlock `bloqueoValuesMutex` before returning this error
		return fmt.Errorf("bloqueo en la posición %d para clave '%s'", pos, key)
	}
	bloqueoValues[pos] = key // Mark this position as being handled
	bloqueoValuesMutex.Unlock()

	defer func(p int64) {
		bloqueoValuesMutex.Lock() // Re-acquire lock to modify the map
		delete(bloqueoValues, p)
		bloqueoValuesMutex.Unlock()
		// println("bloqueoValuesMutex - Position", p, "unblocked.")
	}(pos)

	_, err = fileValues.Seek(pos, io.SeekStart)
	if err != nil {
		fmt.Println("Error al posicionar el archivo Values para escritura:", err)
		return errors.New("error al posicionar el archivo Values para escritura")
	}

	var buf bytes.Buffer

	valueBytes := make([]byte, tamaño)
	copy(valueBytes, []byte(value))
	err = binary.Write(&buf, binary.LittleEndian, valueBytes) // Write the padded bytes
	if err != nil {
		fmt.Println("Error al serializar el valor en buffer:", err)
		return err
	}

	posKey, err := writeKeys(key, pos, tamaño)
	if err != nil {
		fmt.Println("Error al escribir en el archivo Keys:", err)
		return errors.New("error al escribir en el archivo Keys")
	}

	n, err := fileValues.Write(buf.Bytes())
	if err != nil {
		fmt.Println("Error al escribir en el archivo values.db:", err)
		return errors.New("error al escribir en el archivo values.db")
	}

	tablaHashMutex.Lock() // Acquire write lock for the hash table
	defer tablaHashMutex.Unlock()

	// println("Clave: %s Valor: %s PosicionValue: %d PosicionKey: %d Tamaño: %d", key, value, pos, posKey, tamaño)
	tablaHash[key] = DatosDiccionario{Clave: key, Valor: value, PosicionValue: pos, Tamaño: tamaño, PosicionKey: posKey}
	fmt.Printf("Se escribieron %d bytes en el final del archivo Values.\n", n)

	return nil
}

func getAllValuesToDict() error {
	var fileKeys, err = os.OpenFile("./db/keys.db", os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		fmt.Println("Error al abrir/crear el archivo Keys de la DB:", err)
		return errors.New("error al abrir/crear el archivo Keys de la DB")
	}
	defer fileKeys.Close() // Asegura que el archivo se cierre

	fmt.Println("Archivo Keys abierto/creado exitosamente para la lectura.")

	var buf = make([]byte, InfClaveSize)
	_, err = fileKeys.Seek(0, io.SeekStart)
	if err != nil {
		fmt.Println("Error al posicionar el archivo Keys para carga:", err)
		return errors.New("error al posicionar el archivo Keys para carga")
	}

	// Create a temporary map to store data before acquiring the lock
	tempMap := make(map[string]DatosDiccionario)

	for {
		posKey, _ := fileKeys.Seek(0, io.SeekCurrent) // Get current position before read
		n, err := fileKeys.Read(buf)

		if err != nil {
			if err == io.EOF {
				// fmt.Println("Se ha leido el último elemento del archivo Keys durante la carga.")
				break
			}
			// Handle partial reads as potential corruption
			if n > 0 && n < InfClaveSize {
				fmt.Printf("Error al leer el archivo Keys durante la carga: registro incompleto, se leyeron %d de %d bytes. Saltando.\n", n, InfClaveSize)
				// Seek past the incomplete record to try to read the next one
				_, seekErr := fileKeys.Seek(int64(InfClaveSize-n), io.SeekCurrent)
				if seekErr != nil {
					fmt.Println("Fatal: No se pudo saltar el registro incompleto, abortando carga.")
					return fmt.Errorf("error irrecuperable al leer keys.db: %v", err)
				}
				continue // Skip this corrupted record
			}
			fmt.Println("Error al leer el archivo Keys durante la carga:", err)
			return errors.New("error al leer el archivo Keys durante la carga")
		}
		if n < InfClaveSize { // This indicates an unexpected short read without EOF
			fmt.Printf("Advertencia: Se leyeron menos bytes de lo esperado (%d de %d) del archivo Keys durante la carga. Saltando.\n", n, InfClaveSize)
			continue
		}

		var temp InfClave
		reader := bytes.NewReader(buf)
		err = binary.Read(reader, binary.LittleEndian, &temp)
		if err != nil {
			fmt.Println("Error al deserializar InfClave durante la carga:", err)
			continue // Skip this record if it's malformed
		}

		claveString := strings.TrimRight(string(temp.Clave[:]), "\x00")
		valor, err := getValue(temp.Direccion, temp.Tamaño)
		if err != nil {
			fmt.Printf("Error al leer el valor asociado a la clave '%s' durante la carga: %v. Saltando.\n", claveString, err)
			continue // Skip this key if its value can't be read
		}

		if err != nil {
			fmt.Println("Error al leer el archivo:", err)
			return errors.New("error al leer el archivo")
		}

		// Add to a temporary map first
		tempMap[claveString] = DatosDiccionario{Clave: claveString, Valor: valor, PosicionValue: temp.Direccion, Tamaño: temp.Tamaño, PosicionKey: posKey}
	}

	// After reading all data from the file, acquire the lock once to update tablaHash
	tablaHashMutex.Lock()
	defer tablaHashMutex.Unlock() // Ensure it's unlocked even if something goes wrong here

	// Copy all data from tempMap to tablaHash
	for k, v := range tempMap {
		tablaHash[k] = v
	}

	return nil
}

type server struct {
	pb.UnimplementedBDServer
}

func (s *server) GetPrefix(ctx context.Context, in *pb.Consultar) (*pb.RespuestaGetPrefix, error) {
	res, err := searchKeyPrefix(in.Clave, WHERE_HAST_TABLE)
	if err != nil {
		return nil, err
	}

	return &pb.RespuestaGetPrefix{Estado: true, Mensaje: "OK", Objetos: res}, nil
}

func (s *server) Get(ctx context.Context, in *pb.Consultar) (*pb.RespuestaGet, error) {
	value, err := searchKey(in.Clave, WHERE_HAST_TABLE)

	if err != nil {
		if strings.Contains(err.Error(), "clave no encontrada") {
			return &pb.RespuestaGet{Estado: false, Mensaje: "Clave no encontrada"}, nil
		}
		return nil, err
	}

	return &pb.RespuestaGet{Estado: true, Mensaje: "OK", Objeto: &pb.Objeto{Clave: in.Clave, Valor: value}}, nil
}

func (s *server) Set(ctx context.Context, in *pb.Insertar) (*pb.RespuestaSet, error) {
	err := writeValues(in.Clave, in.Valor)

	if err != nil {
		if strings.Contains(err.Error(), "bloqueo") {
			return &pb.RespuestaSet{Estado: false, Mensaje: fmt.Sprintf("Error concurrente: %v", err)}, nil
		}
		return nil, err
	}

	return &pb.RespuestaSet{Estado: true, Mensaje: "OK"}, nil
}

func (s *server) ResetDb(ctx context.Context, in *pb.RequestResetDb) (*pb.RespuestaReset, error) {
	fmt.Println("Solicitud ResetDb recibida.")
	// Acquire write lock for tablaHashMutex before clearing the map
	tablaHashMutex.Lock()
	defer tablaHashMutex.Unlock()

	// Use os.RemoveAll for the 'db' directory to ensure everything inside is gone
	err := os.RemoveAll("./db")
	if err != nil && !os.IsNotExist(err) {
		fmt.Println("Error al eliminar el directorio 'db':", err)
		return nil, err
	}

	// Recreate the 'db' directory
	err = os.Mkdir("./db", 0755)
	if err != nil {
		log.Printf("Error al crear el directorio 'db' después de ResetDb: %v", err)
		return nil, err
	}

	// Reinitialize the map after deleting files
	tablaHash = make(map[string]DatosDiccionario)
	fmt.Println("Base de datos reiniciada exitosamente.")
	return &pb.RespuestaReset{Estado: true, Mensaje: "OK"}, nil
}

func main() {

	flag.Parse() // Parse command-line flags
	// Ensure the 'db' directory exists at startup
	if _, err := os.Stat("./db"); os.IsNotExist(err) {
		err := os.Mkdir("./db", 0755)
		if err != nil {
			log.Fatalf("fallo al crear el directorio 'db': %v", err)
		}
	}

	start := time.Now()
	println("Iniciando la carga de datos a la memoria")
	getAllValuesToDict()

	fmt.Println("Datos cargados a la memoria")
	fmt.Println("Cantidad de clave valor cargados a la memoria:", len(tablaHash))
	end := time.Now()

	fmt.Println("Tiempo de carga:", end.Sub(start))

	lis, err := net.Listen("tcp", ":5050")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer(
		grpc.MaxRecvMsgSize(1024*1024*1024),
		grpc.MaxSendMsgSize(1024*1024*1024),
	)
	pb.RegisterBDServer(s, &server{})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
