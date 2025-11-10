package com.P2PBRS.peer;

import java.io.*;
import java.net.Socket;
import java.util.zip.CRC32;

public class TCPClient {
    public static boolean sendChunk(String storagePeerIP, int storagePeerTCPPort, int requestId, String fileName, int chunkId, byte[] chunkData, String checksum) {
        try (Socket socket = new Socket(storagePeerIP, storagePeerTCPPort); OutputStream out = socket.getOutputStream(); InputStream in = socket.getInputStream()) {
            // Format: SEND_CHUNK RQ# File_Name Chunk_ID Chunk_Size Checksum
            String header = String.format("SEND_CHUNK %d %s %d %d %s\n", requestId, fileName, chunkId, chunkData.length, checksum);

            // Send header
            out.write(header.getBytes());
            out.flush();

            // Send chunk data
            out.write(chunkData);
            out.flush();

            // Wait for response
            byte[] bufferResponse = new byte[1024];
            int bytesRead = in.read(bufferResponse);
            if (bytesRead > 0) {
                String response = new String(bufferResponse, 0, bytesRead).trim();
                return response.equals("CHUNK_OK " + requestId);
            } else {
                return false;
            }

        } catch (IOException e) {
            System.err.println("Failed to send chunk to " + storagePeerIP + ":" + storagePeerTCPPort + " - " + e.getMessage());
            return false;
        }
    }

    public static String calculateChunkChecksum(byte[] data) {
        CRC32 crc32 = new CRC32();
        crc32.update(data);
        return Long.toHexString(crc32.getValue());
    }

    public static byte[] readFileChunk(String filePath, int chunkId, int chunkSize) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(filePath, "r")) {
            long startPos = (long) chunkId * chunkSize;
            long fileSize = file.length();
            
            // Check if we're beyond the file size
            if (startPos >= fileSize) {
                return new byte[0]; // No data for this chunk
            }
            
            // Calculate actual chunk size (last chunk might be smaller)
            int actualChunkSize = (int) Math.min(chunkSize, fileSize - startPos);
            byte[] chunkData = new byte[actualChunkSize];
            
            // Seek to the correct position and read
            file.seek(startPos);
            int bytesRead = file.read(chunkData);
            
            if (bytesRead != actualChunkSize) {
                throw new IOException("Failed to read complete chunk. Expected: " + 
                    actualChunkSize + ", got: " + bytesRead);
            }
            
            return chunkData;
        }
    }

}
