package com.P2PBRS.peer;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;

public class UDPClient {
    private final String serverHost;
    private final int serverPort;
    private int timeoutMs = 2000; // default 2s

    public UDPClient(String serverHost, int serverPort) {
        this.serverHost = serverHost;
        this.serverPort = serverPort;
    }

    public void setTimeout(int timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    private String sendAndReceive(String payload) throws IOException {
        byte[] data = payload.getBytes(StandardCharsets.UTF_8);
        InetAddress addr = InetAddress.getByName(serverHost);

        try (DatagramSocket socket = new DatagramSocket()) {
            socket.setSoTimeout(timeoutMs);
            socket.send(new DatagramPacket(data, data.length, addr, serverPort));

            byte[] buf = new byte[2048];
            DatagramPacket resp = new DatagramPacket(buf, buf.length);
            try {
                socket.receive(resp);
                return new String(resp.getData(), 0, resp.getLength(), StandardCharsets.UTF_8);
            } catch (SocketTimeoutException e) {
                return "ERROR: No response from server (timeout)";
            }
        }
    }

    private String sendCommand(int rqNumber, String command, String... args) throws IOException {
        StringBuilder sb = new StringBuilder(64);
        sb.append(command).append(' ').append(rqNumber);
        if (args != null) {
            for (String a : args) sb.append(' ').append(a == null ? "" : a);
        }
        return sendAndReceive(sb.toString());
    }

    public String sendRegister(int rqNumber, PeerNode node) throws Exception {
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
}
