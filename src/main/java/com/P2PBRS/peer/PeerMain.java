package com.P2PBRS.peer;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.CRC32;

import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.DefaultParser;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

public class PeerMain {
    private static int request = 0;
    private static final Map<String, String> storagePeerIps = new ConcurrentHashMap<>(); // maps storage peer name -> IP
    private static final Map<String, Integer> storagePeerPorts = new ConcurrentHashMap<>(); // maps storage peer name -> Port


    public static void main(String[] args) throws Exception {
        if (args.length == 0 || !"register".equals(args[0])) {
            helpStartup();
            System.exit(1);
        }

        if (args.length < 7) {
            helpStartup();
            System.exit(1);
        }
        String name = args[1];
        String role = args[2];
        String ip = args[3];
        int udpPort = Integer.parseInt(args[4]);
        int tcpPort = Integer.parseInt(args[5]);
        int storage = Integer.parseInt(args[6]);
        String host = args.length > 7 ? args[7] : "localhost";
        int port = args.length > 8 ? Integer.parseInt(args[8]) : 5000;
        Integer timeoutOpt = args.length > 9 ? Integer.parseInt(args[9]) : null;

        PeerNode self = new PeerNode(name, role, ip, udpPort, tcpPort, storage);

        // IMPORTANT: bind UDP client to the registered udpPort
        UDPClient client = new UDPClient(host, port, udpPort);
        if (timeoutOpt != null) client.setTimeout(timeoutOpt);

        String regResp = client.sendRegister(request++, self);
        System.out.println("Server Response: " + regResp);
        if (!regResp.startsWith("REGISTERED")) {
            System.err.println("Registration failed, exiting.");
            client.close();
            System.exit(2);
        }

        Path storageDir = null;
        if ("STORAGE".equalsIgnoreCase(role) || "BOTH".equalsIgnoreCase(role)) {
            storageDir = Path.of("storage_" + name);
            Files.createDirectories(storageDir); // make sure folder exists
        }

        if (storageDir != null) {
            Path finalStorageDir = storageDir; // for lambda capture
            client.setUnsolicitedHandler((msg, from) -> {
            try {
                if (msg.startsWith("STORE_REQ")) {
                    // Expected format: STORE_REQ <RQ#> <FileName> <ChunkID> <OwnerPeer>
                    String[] parts = msg.split("\\s+");
                    System.out.println("DEBUG STORE_REQ: " + Arrays.toString(parts));
                    
                    if (parts.length < 5) {
                        System.err.println("Malformed STORE_REQ: " + msg);
                        return;
                    }

                    String rq = parts[1];
                    String fileName = parts[2];
                    int chunkId = Integer.parseInt(parts[3]);
                    String ownerPeer = parts[4];
                    
                    System.out.println("STORE_REQ: Ready for chunk " + chunkId + " of " + fileName + " from " + ownerPeer); // The actual chunk will come via TCP from the owner
                } else if (msg.startsWith("STORAGE_TASK")) {
                    // Expected format: STORAGE_TASK <RQ#> <FileName> <ChunkSize> <OwnerPeer>
                    String[] parts = msg.split("\\s+");
                    System.out.println("DEBUG STORAGE_TASK: " + Arrays.toString(parts));
                    
                    if (parts.length < 5) {
                        System.err.println("Malformed STORAGE_TASK: " + msg);
                        return;
                    }

                    String rq = parts[1];
                    String fileName = parts[2];
                    int chunkSize = Integer.parseInt(parts[3]);
                    String ownerPeer = parts[4];
                    
                    System.out.println("STORAGE_TASK: Will receive " + fileName + " with chunk size " + chunkSize + " from " + ownerPeer);
                    
                } else {
                    System.out.println("[unsolicited] " + from + " -> " + msg);
                }
            } catch (Exception e) {
                System.err.println("Failed to handle unsolicited message: " + e.getMessage());
                e.printStackTrace();
            }
        });

            // Start TCP server for incoming chunk storage requests
            new Thread(() -> {
                try (ServerSocket serverSocket = new ServerSocket(tcpPort)) {
                    System.out.println("TCP storage server listening on port " + tcpPort);
                    while (true) {
                        Socket socket = serverSocket.accept();
                        new Thread(() -> handleIncomingChunk(socket, finalStorageDir)).start();
                    }
                } catch (Exception e) {
                    System.err.println("TCP server failed: " + e.getMessage());
                }
            }, "Storage-TCP-Server").start();
        }


        // Interactive CLI
        System.out.println("\n== Peer CLI (registered as " + name + ", role " + role + ") ==");
        printHelpInCli();

        Terminal terminal = TerminalBuilder.builder()
            .system(true)
            .build();

        LineReader reader = LineReaderBuilder.builder()
            .terminal(terminal)
            .parser(new DefaultParser())
            .variable(LineReader.HISTORY_FILE, java.nio.file.Paths.get(System.getProperty("user.home"), ".peercli_history"))
            .history(new DefaultHistory())
            .build();

        while (true) {
            String line;
            try {
                line = reader.readLine("> ");  // handles arrow keys and editing
            } catch (UserInterruptException | EndOfFileException e) {
                break; // Ctrl-C or Ctrl-D exits cleanly
            }

            line = line.trim();
            if (line.isEmpty()) continue;

            String[] toks = line.split("\\s+");
            String cmd = toks[0].toLowerCase();

            String resp;

            try {
                switch (cmd) {
                    case "help":
                        printHelpInCli();
                        break;
                    case "backup":
                        // backup <FilePath> <ChunkSizeBytes>
                        if (toks.length < 3) {
                            System.out.println("usage: backup <FilePath> <ChunkSizeBytes>");
                            break;
                        }
                        Path filePath = Path.of(toks[1]);
                        int chunkSize = Integer.parseInt(toks[2]);

                        if (!Files.exists(filePath) || !Files.isRegularFile(filePath)) {
                            System.out.println("ERROR: file not found: " + filePath);
                            break;
                        }

                        long fileSize = Files.size(filePath);
                        String fileName = filePath.getFileName().toString();

                        CRC32 crc = new CRC32();
                        try (FileInputStream fis = new FileInputStream(filePath.toFile())) {
                            byte[] buf = new byte[64 * 1024];
                            int n;
                            while ((n = fis.read(buf)) > 0) crc.update(buf, 0, n);
                        }
                        String checksumHex = Long.toHexString(crc.getValue());

                        resp = client.sendBackupReq(request++, fileName, fileSize, checksumHex, chunkSize);
                        System.out.println("Server Response: " + resp);

                        // Parse BACKUP_PLAN with connection details
                        List<String> assignedPeers = new ArrayList<>();
                        int planChunkSize = chunkSize;

                        if (resp.startsWith("BACKUP_PLAN")) {
                            System.out.println("BACKUP_PLAN parsing: " + resp);
                            
                            int startBracket = resp.indexOf('[');
                            int endBracket = resp.indexOf(']');
                            if (startBracket >= 0 && endBracket > startBracket) {
                                String peersStr = resp.substring(startBracket + 1, endBracket);
                                System.out.println("  - Raw peer string: '" + peersStr + "'");
                                
                                String[] peerEntries = peersStr.split(",");
                                
                                for (String peerEntry : peerEntries) {
                                    peerEntry = peerEntry.trim();
                                    System.out.println("  - Parsing peer entry: '" + peerEntry + "'");
                                    
                                    String[] parts = peerEntry.split(":");
                                    
                                    if (parts.length == 3) {
                                        // Format: PeerName:IP:TCPPort
                                        String peerName = parts[0];
                                        String peerIp = parts[1];
                                        int peerTcpPort = Integer.parseInt(parts[2]);
                                        
                                        assignedPeers.add(peerName);
                                        storagePeerIps.put(peerName, peerIp);
                                        storagePeerPorts.put(peerName, peerTcpPort);
                                        
                                        System.out.println("  - Mapped " + peerName + " -> " + peerIp + ":" + peerTcpPort + " (from server)");
                                    } else {
                                        // Fallback for unexpected format
                                        System.err.println("  - WARNING: Unexpected peer format: " + peerEntry);
                                        String peerName = peerEntry;
                                        assignedPeers.add(peerName);
                                        // Don't set fallback - we want to see the error clearly
                                    }
                                }
                            } else {
                                System.err.println("  - ERROR: Could not find peer list in brackets");
                            }
                            
                            // Parse chunk size
                            String afterBracket = resp.substring(endBracket + 1).trim();
                            String[] tokens = afterBracket.split("\\s+");
                            if (tokens.length > 0) {
                                try {
                                    planChunkSize = Integer.parseInt(tokens[tokens.length - 1]);
                                    System.out.println("  - Chunk size: " + planChunkSize);
                                } catch (NumberFormatException e) {
                                    System.err.println("Warning: Could not parse chunk size from BACKUP_PLAN");
                                }
                            }
                        }

                        // Validate we have connection info for all peers
                        for (String peerName : assignedPeers) {
                            if (!storagePeerIps.containsKey(peerName) || !storagePeerPorts.containsKey(peerName)) {
                                System.err.println("ERROR: Missing connection info for peer: " + peerName);
                                System.err.println("Cannot proceed with backup - no IP/port mapping");
                                return;
                            }
                        }


                        System.out.println("BACKUP_PLAN parsed:");
                        System.out.println("  - File: " + fileName);
                        System.out.println("  - Assigned peers: " + assignedPeers);
                        System.out.println("  - Chunk size: " + planChunkSize);

                        try (FileInputStream fis = new FileInputStream(filePath.toFile())) {
                            byte[] buffer = new byte[chunkSize];
                            int bytesRead;
                            int chunkId = 0;

                            while ((bytesRead = fis.read(buffer)) > 0) {
                                byte[] chunk = Arrays.copyOf(buffer, bytesRead);
                                System.out.println("Prepared chunk " + chunkId + " of size " + bytesRead);

                                for (int i = 0; i < assignedPeers.size(); i++) {
                                    String peerName = assignedPeers.get(i);
                                    String peerIp = storagePeerIps.get(peerName);
                                    int peerPort = storagePeerPorts.get(peerName);

                                    boolean sent = false;
                                    int attempts = 0;
                                    while (!sent && attempts < 3) {
                                        attempts++;
                                        try (Socket tcpSocket = new Socket(peerIp, peerPort);
                                            OutputStream out = tcpSocket.getOutputStream()) {

                                            // send metadata header
                                            String header = request + " " + fileName + " " + chunkId + " " + chunk.length + " " + checksumHex + "\n";
                                            out.write(header.getBytes(StandardCharsets.UTF_8));
                                            out.write(chunk);
                                            out.flush();

                                            // TODO: wait for CHUNK_OK or CHUNK_ERROR via UDP
                                            sent = true; // mark sent for now; we'll add UDP ack next

                                        } catch (Exception e) {
                                            System.err.println("Failed to send chunk " + chunkId + " to " + peerName + " attempt " + attempts);
                                        }
                                    }
                                    if (!sent) {
                                        System.err.println("Giving up on chunk " + chunkId + " for peer " + peerName);
                                    }
                                }

                                chunkId++;
                            }
                        }

                        resp = client.sendBackupDone(request++, fileName);
                        System.out.println("Server Response: " + resp);
                        break;

                    case "deregister":
                    case "exit":
                    case "quit":
                        try {
                            resp = client.sendDeregister(request++, name);
                            System.out.println("[shutdown] Server Response: " + resp);
                        } catch (Exception e) {
                            System.err.println("[shutdown] Failed to de-register: " + e.getMessage());
                        } finally {
                            client.close();
                        }
                        return;

                    default:
                        System.out.println("Unknown command: " + cmd);
                        printHelpInCli();
                }
            } catch (Exception e) {
                System.err.println("Command failed: " + e.getMessage());
            }
        }
    }

    public static void helpStartup() {
    System.out.println(
        "Startup usage:\n" +
        "  register <Name> <OWNER|STORAGE|BOTH> <IP_Address> <UDP_Port> <TCP_Port> <Capacity Bytes> [<ServerHost>] [<ServerPort>] [<timeout ms>]\n" +
        "\n" +
        "Example:\n" +
        "  register Alice BOTH 192.168.1.10 5001 6001 104857600 localhost 5000 2000\n"
    );
}

private static void printHelpInCli() {
    System.out.println(
        "Commands:\n" +
        "  backup <FilePath> <ChunkSizeBytes>   # announce backup to server\n" +
        "  deregister                           # de-register now and exit\n" +
        "  help                                 # show this\n" +
        "  exit | quit                          # exit (auto de-register)\n"
    );
}

private static void handleIncomingChunk(Socket socket, Path storageDir) {
    try (socket; InputStream in = socket.getInputStream()) {
        Scanner scanner = new Scanner(in, StandardCharsets.UTF_8.name());
        String header = scanner.nextLine(); // RQ# File_Name Chunk_ID Chunk_Size Checksum
        String[] parts = header.split("\\s+");
        int rq = Integer.parseInt(parts[0]);
        String fileName = parts[1];
        int chunkId = Integer.parseInt(parts[2]);
        int chunkSize = Integer.parseInt(parts[3]);
        long expectedCrc = Long.parseLong(parts[4], 16);

        // read exactly chunkSize bytes
        byte[] chunkData = in.readNBytes(chunkSize);

        // verify CRC32
        CRC32 crc = new CRC32();
        crc.update(chunkData);
        if (crc.getValue() != expectedCrc) {
            System.out.println("Checksum mismatch for " + fileName + " chunk " + chunkId);
            // send CHUNK_ERROR via UDP
            // TODO: need owner's address from STORE_REQ message context
            return;
        }

        // store chunk
        Path fileFolder = storageDir.resolve(fileName);
        Files.createDirectories(fileFolder);
        Path chunkFile = fileFolder.resolve("chunk" + chunkId);
        Files.write(chunkFile, chunkData);
        System.out.println("Stored chunk " + chunkId + " of file " + fileName);

        // send CHUNK_OK via UDP
        // TODO: need owner's address from STORE_REQ message context

    } catch (Exception e) {
        System.err.println("Failed to handle TCP chunk: " + e.getMessage());
    }
}

}
