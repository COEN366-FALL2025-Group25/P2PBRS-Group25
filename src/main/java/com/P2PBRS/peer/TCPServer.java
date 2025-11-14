package com.P2PBRS.peer;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.CRC32;

public class TCPServer extends Thread {
    private final int tcpPort;
    private final UDPClient udpClient;
    private volatile boolean running = true;

    public TCPServer(int tcpPort, UDPClient udpClient) {
        this.tcpPort = tcpPort;
        this.udpClient = udpClient;
    }

    @Override
    public void run() {
        try (ServerSocket serverSocket = new ServerSocket(tcpPort)) {
            System.out.println("TCP Server started on port " + tcpPort);

            while (running) {
                Socket clientSocket = serverSocket.accept();
                new ChunkHandler(clientSocket, udpClient).start();
            }

        } catch (IOException e) {
            System.err.println("TCP Server error: " + e.getMessage());
        }
    }

    public void stopServer() {
        running = false;
        try {
            new Socket("localhost", tcpPort).close(); // Trigger accept() to exit
        } catch (IOException e) {
            // Ignore
        }
    }

    static class ChunkHandler extends Thread {
        private final Socket socket;
        private final UDPClient udpClient;

        public ChunkHandler(Socket socket, UDPClient udpClient) {
            this.socket = socket;
            this.udpClient = udpClient;
        }

        @Override
        public void run() {
            try (InputStream in = socket.getInputStream(); OutputStream out = socket.getOutputStream()) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(in));

                String header = reader.readLine();
                if (header == null) {
                    System.out.println("Empty TCP request, closing connection");
                    return;
                }
                System.out.println("Received TCP chunk header: " + header);

                String[] parts = header.split(" ");
                if (parts.length < 6 || !parts[0].equals("SEND_CHUNK")) {
                    out.write("ERROR: Invalid command\n".getBytes());
                    return;
                }

                int requestId = Integer.parseInt(parts[1]);
                String fileName = parts[2];
                int chunkId = Integer.parseInt(parts[3]);
                int chunkSize = Integer.parseInt(parts[4]);
                String expectedChecksum = parts[5];

                // Read chunk data
                byte[] chunkData = new byte[chunkSize];
                int totalRead = 0;
                while (totalRead < chunkSize) {
                    int bytesRead = in.read(chunkData, totalRead, chunkSize - totalRead);
                    if (bytesRead == -1) break;
                    totalRead += bytesRead;
                }

                if (totalRead != chunkSize) {
                     // Send TCP error response
                    out.write(("CHUNK_ERROR " + requestId + " " + fileName + " " + chunkId + " Incomplete_Data\n").getBytes());
                    
                    // Notify server via UDP
                    udpClient.sendChunkError(requestId, fileName, chunkId, "Incomplete_Data");

                    System.out.println("ERROR: Incomplete chunk data for " + fileName + " chunk " + chunkId);
                    return;
                }

                // Verify checksum
                String actualChecksum = TCPClient.calculateChunkChecksum(chunkData);
                if (!actualChecksum.equals(expectedChecksum)) {
                    // Send TCP error response (protocol format)
                    out.write(("CHUNK_ERROR " + requestId + " " + fileName + " " + chunkId + " Checksum_Mismatch\n").getBytes());                    
                    System.out.println("Checksum mismatch for chunk " + chunkId + " of file " + fileName);
                    
                    // Notify server via UDP
                    udpClient.sendChunkError(requestId, fileName, chunkId, "Checksum_Mismatch");

                    System.out.println("Checksum mismatch for chunk " + chunkId + " of file " + fileName);
                    return;
                }

                // Store chunk locally
                if (storeChunk(fileName, chunkId, chunkData)) {
                    // Send TCP success response (protocol format)
                    out.write(("CHUNK_OK " + requestId + " " + fileName + " " + chunkId + "\n").getBytes());
                    
                    // Notify server via UDP
                    udpClient.sendChunkOk(requestId, fileName, chunkId);
                    
                    System.out.println("✓ Stored chunk " + chunkId + " of file " + fileName);
                } else {
                    out.write(("CHUNK_ERROR " + requestId + " " + fileName + " " + chunkId + " Storage_Failed\n").getBytes());
                    udpClient.sendChunkError(requestId, fileName, chunkId, "Storage_Failed");                
                }

            } catch (IOException e) {
                System.err.println("ChunkHandler error: " + e.getMessage());
            } finally {
                try {
                    socket.close();
                } catch (IOException e) {
                    // Ignore
                }
            }
        }

        private boolean storeChunk(String fileName, int chunkId, byte[] chunkData) {
            try {
                Path chunkDir = Paths.get("chunks", fileName);
                Files.createDirectories(chunkDir);

                Path chunkFile = chunkDir.resolve("chunk_" + chunkId);
                Files.write(chunkFile, chunkData);
                return true;
            } catch (IOException e) {
                System.err.println("Failed to store chunk: " + e.getMessage());
                return false;
            }
        }
        
    }
}
