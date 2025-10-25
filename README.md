# P2PBRS - Peer-to-Peer Backup and Recovery System

## Team
- Member 1: Server Core & GitHub Setup
- Member 2: Peer Client & UDP Communication
- Member 3: Persistence & Data Models  
- Member 4: Logging & Testing

## Build & Run
```bash
P2PBRS-Group25 % brew install maven
P2PBRS-Group25 % mvn compile
P2PBRS-Group25 % mvn exec:java

## Tests
P2PBRS-Group25 % cd src/tests
tests % javac TestClient.java
tests % java TestClient    