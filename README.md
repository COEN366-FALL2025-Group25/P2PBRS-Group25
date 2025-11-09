# P2PBRS - Peer-to-Peer Backup and Recovery System


## Build & Run
```bash
P2PBRS-Group25 % brew install maven
P2PBRS-Group25 % mvn compile
P2PBRS-Group25 % mvn exec:java -Dexec.args="-h"

# Server
P2PBRS-Group25 % mvn exec:java -Dexec.args="server"

# Peer - Start client
P2PBRS-Group25 % mvn exec:java -Dexec.args="peer register <Name> <OWNER|STORAGE|BOTH> <IP_Address> <UDP_Port> <TCP_Port> <Capacity Bytes>"
P2PBRS-Group25 % mvn exec:java -Dexec.args="peer register Alice BOTH 192.168.1.10 5001 6001 1024"

# Peer - in CLI
> backup <file_name> <chunk_size>
> buckup README.md 4096

# Peer - exit session
> quit
> exit
> deregister

## Tests
P2PBRS-Group25 % cd src/tests
tests % javac TestClient.java
tests % java TestClient
```

## Project

### Architecure

- Single application built into both a client and server handler to improve the code-reusability
- Free running thread based server to handle each client request
- Blocking client to handle individual transmissions to the main server


### Server

- Registry maintained locally to handle persistent sessions across server instances
- Client handler runs threaded contexts to handle Client <-> Server interactions in multi-client environments
