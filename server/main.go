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
	"strconv"
	"strings"
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

func getValue(pos int64, tamaño int32) (string, error) {
	var fileValues, err = os.OpenFile("./db/values.db", os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		fmt.Println("Error al abrir/crear el archivo Values de la DB:", err)
		return "", errors.New("error al abrir/crear el archivo Values de la DB")
	}
	defer fileValues.Close()

	fmt.Println("Archivo Values abierto/creado exitosamente.")

	var buf = make([]byte, tamaño)
	fileValues.Seek(pos, io.SeekStart)

	_, err = fileValues.Read(buf)

	if err != nil {
		fmt.Println("Error al leer el archivo:", err)
		return "", errors.New("error al leer el archivo")
	}

	return string(buf), nil
}

func searchKey(key string, where int8) (string, error) {
	if where == WHERE_FILESYSTEM {
		var fileKeys, err = os.OpenFile("./db/keys.db", os.O_RDWR|os.O_CREATE, 0644)

		if err != nil {
			fmt.Println("Error al abrir/crear el archivo Keys de la DB:", err)
			return "", errors.New("error al abrir/crear el archivo Keys de la DB")
		}
		defer fileKeys.Close()

		fmt.Println("Archivo Keys abierto/creado exitosamente para la lectura.")

		var buf = make([]byte, InfClaveSize)
		fileKeys.Seek(0, io.SeekStart)

		var clave InfClave
		claveEncontrada := false

		for {
			_, err := fileKeys.Read(buf)

			if err != nil {
				if err == io.EOF {
					fmt.Println("Se ha leido el último elemento del archivo")
					break
				}
				fmt.Println("Error al leer el archivo:", err)
				return "", errors.New("error al leer el archivo")
			}

			var temp InfClave
			reader := bytes.NewReader(buf)
			err = binary.Read(reader, binary.LittleEndian, &temp)

			if err != nil {
				fmt.Println("Error al leer el archivo:", err)
				return "", errors.New("error al leer el archivo")
			}

			println(string(temp.Clave[:16]) + "   " + strconv.FormatInt(temp.Direccion, 10) + "   " + strconv.FormatInt(int64(temp.Tamaño), 10))
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
			fmt.Println("Error al leer el archivo:", err)
			return "", errors.New("error al leer el archivo")
		}

		return valor, nil
	}

	if where == WHERE_HAST_TABLE {
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

		fmt.Println("Archivo Keys abierto/creado exitosamente para la lectura.")

		var buf = make([]byte, InfClaveSize)
		fileKeys.Seek(0, io.SeekStart)

		var claves []InfClave

		for {
			_, err := fileKeys.Read(buf)

			if err != nil {
				if err == io.EOF {
					fmt.Println("Se ha leido el último elemento del archivo")
					break
				}
				fmt.Println("Error al leer el archivo:", err)
				return []*pb.Objeto{}, errors.New("error al leer el archivo")
			}

			var temp InfClave
			reader := bytes.NewReader(buf)
			err = binary.Read(reader, binary.LittleEndian, &temp)

			if err != nil {
				fmt.Println("Error al leer el archivo:", err)
				return []*pb.Objeto{}, errors.New("error al leer el archivo")
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

	tableValue, exist := tablaHash[key]
	var pos int64

	if !exist {
		pos, _ = fileKeys.Seek(0, io.SeekEnd)

	} else {
		pos, _ = fileKeys.Seek(tableValue.PosicionKey, io.SeekStart)
	}

	fmt.Println("Archivo Keys abierto/creado para edicion exitosamente.")

	var buf bytes.Buffer

	var temp InfClave

	copy(temp.Clave[:], []byte(key))
	temp.Direccion = posicion
	temp.Tamaño = tamaño

	err = binary.Write(&buf, binary.LittleEndian, temp)

	if err != nil {
		fmt.Println("Error al escribir en el archivo:", err)
		return -1, err
	}

	n, err := fileKeys.Write(buf.Bytes())
	if err != nil {
		fmt.Println("Error al escribir en el archivo:", err)
		return -1, err
	}

	fmt.Printf("Se escribieron %d bytes en el final del archivo.\n", n)

	return pos, nil
}

func writeValues(key string, value string) error {
	var fileValues, err = os.OpenFile("./db/values.db", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		fmt.Println("Error al abrir/crear el archivo Values de la DB:", err)
		return errors.New("error al abrir/crear el archivo Values de la DB")
	}
	defer fileValues.Close() // Asegura que el archivo se cierre

	fmt.Println("Archivo Values abierto/creado exitosamente.")

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

	pos, _ := fileValues.Seek(0, io.SeekEnd)

	var buf bytes.Buffer

	if tamaño == B512 {
		var temp [B512]byte
		copy(temp[:], []byte(value))
		err = binary.Write(&buf, binary.LittleEndian, temp)
	} else if tamaño == KB4 {
		var temp [KB4]byte
		copy(temp[:], []byte(value))
		err = binary.Write(&buf, binary.LittleEndian, temp)
	} else if tamaño == KB512 {
		var temp [KB512]byte
		copy(temp[:], []byte(value))
		err = binary.Write(&buf, binary.LittleEndian, temp)
	} else if tamaño == MB1 {
		var temp [MB1]byte
		copy(temp[:], []byte(value))
		err = binary.Write(&buf, binary.LittleEndian, temp)
	} else if tamaño == MB4 {
		var temp [MB4]byte
		copy(temp[:], []byte(value))
		err = binary.Write(&buf, binary.LittleEndian, temp)
	}

	posKey, err := writeKeys(key, pos, tamaño)
	if err != nil {
		fmt.Println("Error al escribir en el archivo Keys:", err)
		return errors.New("error al escribir en el archivo Keys")
	}

	n, err := fileValues.Write(buf.Bytes())
	if err != nil {
		fmt.Println("Error al escribir en el archivo:", err)
		return errors.New("error al escribir en el archivo")
	}

	tablaHash[key] = DatosDiccionario{Clave: key, Valor: value, PosicionValue: pos, Tamaño: tamaño, PosicionKey: posKey}
	fmt.Printf("Se escribieron %d bytes en el final del archivo.\n", n)

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
	fileKeys.Seek(0, io.SeekStart)

	for {
		pos, _ := fileKeys.Seek(0, io.SeekCurrent)
		_, err := fileKeys.Read(buf)

		if err != nil {
			if err == io.EOF {
				fmt.Println("Se ha leido el último elemento del archivo")
				break
			}
			fmt.Println("Error al leer el archivo:", err)
			return errors.New("error al leer el archivo")
		}

		var temp InfClave
		reader := bytes.NewReader(buf)
		err = binary.Read(reader, binary.LittleEndian, &temp)

		if err != nil {
			fmt.Println("Error al leer el archivo:", err)
			return errors.New("error al leer el archivo")
		}

		claveString := strings.TrimRight(string(temp.Clave[:16]), "\x00")
		valor, err := getValue(temp.Direccion, temp.Tamaño)

		if err != nil {
			fmt.Println("Error al leer el archivo:", err)
			return errors.New("error al leer el archivo")
		}

		tablaHash[claveString] = DatosDiccionario{Clave: claveString, Valor: valor, PosicionValue: temp.Direccion, Tamaño: temp.Tamaño, PosicionKey: pos}
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
	value, _ := searchKey(in.Clave, WHERE_HAST_TABLE)

	return &pb.RespuestaGet{Estado: true, Mensaje: "OK", Objeto: &pb.Objeto{Clave: in.Clave, Valor: value}}, nil
}

func (s *server) Set(ctx context.Context, in *pb.Insertar) (*pb.RespuestaSet, error) {
	err := writeValues(in.Clave, in.Valor)

	if err != nil {
		return nil, err
	}

	return &pb.RespuestaSet{Estado: true, Mensaje: "OK"}, nil
}

func main() {
	start := time.Now()
	getAllValuesToDict()

	fmt.Println("Datos cargados a la memoria")
	fmt.Println("Cantidad de clave valor cargados a la memoria:", len(tablaHash))
	end := time.Now()

	fmt.Println("Tiempo de carga:", end.Sub(start))

	lis, err := net.Listen("tcp", ":5050")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterBDServer(s, &server{})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
