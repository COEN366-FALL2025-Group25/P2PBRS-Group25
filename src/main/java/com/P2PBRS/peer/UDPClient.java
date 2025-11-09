package com.P2PBRS.peer;

import java.io.Closeable;
import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;

public class UDPClient implements Closeable {
    private final String serverHost;
    private final int serverPort;
    private final int localUdpPort;
    private final DatagramSocket socket;   // persistent, bound
    private int timeoutMs = 2000;         // default 2s

    public UDPClient(String serverHost, int serverPort, int localUdpPort) throws SocketException {
        this.serverHost = serverHost;
        this.serverPort = serverPort;
        this.localUdpPort = localUdpPort;
        // Bind to the peer’s registered UDP port so the server can identify us
        this.socket = new DatagramSocket(new InetSocketAddress(localUdpPort));
        this.socket.setReuseAddress(true);
        this.socket.setSoTimeout(timeoutMs);
    }

    public UDPClient setTimeout(int timeoutMs) throws SocketException {
        this.timeoutMs = timeoutMs;
        this.socket.setSoTimeout(timeoutMs);
        return this;
    }

    private String sendAndReceive(String payload) throws IOException {
        byte[] data = payload.getBytes(StandardCharsets.UTF_8);
        InetAddress addr = InetAddress.getByName(serverHost);

        // Send from the already-bound socket; source port stays constant
        socket.send(new DatagramPacket(data, data.length, addr, serverPort));

        byte[] buf = new byte[4096];
        DatagramPacket resp = new DatagramPacket(buf, buf.length);
        try {
            socket.receive(resp);
            return new String(resp.getData(), 0, resp.getLength(), StandardCharsets.UTF_8);
        } catch (SocketTimeoutException e) {
            return "ERROR: No response from server (timeout)";
        }
    }

    private String sendCommand(int rqNumber, String command, String... args) throws IOException {
        StringBuilder sb = new StringBuilder(64);
        sb.append(command).append(' ').append(rqNumber);
        if (args != null) for (String a : args) sb.append(' ').append(a == null ? "" : a);
        return sendAndReceive(sb.toString());
    }

    public String sendRegister(int rqNumber, PeerNode node) throws IOException {
        // REGISTER RQ# Name Role IP_Address UDP_Port# TCP_Port# Storage_Capacity
        return sendCommand(
            rqNumber,
            "REGISTER",
            node.getName(),
            node.getRole(),
            node.getIpAddress(),
            String.valueOf(node.getUdpPort()),
            String.valueOf(node.getTcpPort()),
            String.valueOf(node.getStorageCapacity())
        );
    }

    public String sendDeregister(int rqNumber, String name) throws IOException {
        // DE-REGISTER RQ# Name
        return sendCommand(rqNumber, "DE-REGISTER", name);
    }

    public String sendBackupReq(int rqNumber, String fileName, long fileSize, String checksumHex, int chunkSize) throws IOException {
        // BACKUP_REQ RQ# File_Name File_Size Checksum Chunk_Size
        return sendCommand(
            rqNumber,
            "BACKUP_REQ",
            fileName,
            String.valueOf(fileSize),
            checksumHex,
            String.valueOf(chunkSize)
        );
    }

    public String sendBackupDone(int rqNumber, String fileName) throws IOException {
        // BACKUP_DONE RQ# File_Name
        return sendCommand(rqNumber, "BACKUP_DONE", fileName);
    }

    @Override
    public void close() {
        socket.close();
    }
}
