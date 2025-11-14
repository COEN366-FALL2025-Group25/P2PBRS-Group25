package com.P2PBRS.peer;

import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Scanner;
import java.util.zip.CRC32;

import org.jline.reader.*;
import org.jline.reader.impl.DefaultParser;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

public class PeerMain {
    private static int request = 0;

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

        // Start TCP server to receive chunk data
        if (role.equals("STORAGE") || role.equals("BOTH")) {
            TCPServer tcpServer = new TCPServer(tcpPort, client);
            tcpServer.start();
            System.out.println("TCP Server started on port " + tcpPort);
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

                        // Parse BACKUP_PLAN and send chunks via TCP to each storage peer
                        if (resp.startsWith("BACKUP_PLAN")) {
                            boolean backupSuccess = executeBackupPlan(resp, filePath, chunkSize, client);
                            if (backupSuccess) {
                                resp = client.sendBackupDone(request++, fileName);
                                System.out.println("Backup completion: " + resp);
                            } else {
                                System.out.println("Backup failed - some chunks could not be transferred");
                                break;
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

    private static boolean executeBackupPlan(String backupPlan, Path filePath, int requestChunkSize, UDPClient client) {
        try{
            // Parse: BACKUP_PLAN RQ# File_Name [Peer1, Peer2,...] Chunk_Size
            String[] parts = backupPlan.split("\\s+",5); // Split into max 5 parts
            if (parts.length < 5) {
                System.err.println("Invalid BACKUP_PLAN format");
                return false;
            }

            String rqNumber = parts[1];
            String fileName = parts[2];
            String peersListStr = parts[3]; // [Peer1,Peer2,...]
            int actualChunkSize = Integer.parseInt(parts[4]);

            // Extract peer names
            String peerNames = peersListStr.substring(1, peersListStr.length() - 1); // Remove brackets
            String[] storagePeers = peerNames.split(",");

            if(storagePeers.length == 0) {
                System.err.println("No storage peers assigned in BACKUP_PLAN");
                return false;
            }

            System.out.println("Backup plan: " + storagePeers.length + " peers, chunk size " + actualChunkSize);

            // Calculate total chunks needed
            long fileSize = Files.size(filePath);
            int totalChunks = (int) Math.ceil((double) fileSize / actualChunkSize);
            System.out.println("Total file size: " + fileSize + " bytes, total chunks: " + totalChunks);

            boolean allChunksSuccess = true;

            for (int chunkId = 0; chunkId < totalChunks; chunkId++) {
                byte[] chunkData = TCPClient.readFileChunk(filePath.toString(), chunkId, actualChunkSize);
                if (chunkData.length == 0 && chunkId >= totalChunks) {
                   break; // No more data to read
                }

                // Determine which storage peer to send this chunk to (round-robin)
                String storagePeerName = storagePeers[chunkId % storagePeers.length];

                // TODO: Get the actual IP and TCP port from server
                // Assume TCP port = UDP port + 1000
                String storageIP = "127.0.0.1"; 
                int storageTCPPort = 6002;

                String checksum = TCPClient.calculateChunkChecksum(chunkData);

                System.out.printf("Sending chunk %d to %s at %s:%d%n", chunkId, storagePeerName, storageIP, storageTCPPort);

                // Try sending chunk via TCP
                boolean sent = TCPClient.sendChunk(storageIP, storageTCPPort, Integer.parseInt(rqNumber), fileName, chunkId, chunkData, checksum);
                if (sent) {
                    System.out.println("Successfully sent chunk " + chunkId + " to " + storagePeerName);
                } else {
                    System.err.println("Failed to send chunk " + chunkId + " to " + storagePeerName);
                    allChunksSuccess = false;
                }
            }

            return allChunksSuccess;

        } catch (Exception e) {
            System.err.println("Error executing backup plan: " + e.getMessage());
            return false;
        }
    }
}
