build: server.exe client

server.exe: server/main.go
	go mod tidy
	go build -o server.exe server/main.go

client: client/run_client.py
	python -m PyInstaller --onefile --distpath . client/run_client.py