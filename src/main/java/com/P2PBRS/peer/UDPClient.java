package com.P2PBRS.peer;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;

public class UDPClient {
    private final String serverHost;
    private final int serverPort;
    private int timeoutMs = 2000; // default 2s

    public UDPClient(String serverHost, int serverPort) {
        this.serverHost = serverHost;
        this.serverPort = serverPort;
    }

    public void setTimeoutMs(int timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    public String sendRegister(int rqNumber, PeerNode node) throws Exception {
        // REGISTER message format expected by your server:
        // REGISTER RQ# Name Role IP_Address UDP_Port# TCP_Port# Storage_Capacity
        String message = String.format(
            "REGISTER %d %s %s %s %d %d %s",
            rqNumber,
            node.getName(),
            node.getRole(),
            node.getIpAddress(),
            node.getUdpPort(),
            node.getTcpPort(),
            node.getStorageCapacity()
        );

        try (DatagramSocket socket = new DatagramSocket()) {
            socket.setSoTimeout(timeoutMs);
            InetAddress addr = InetAddress.getByName(serverHost);
            byte[] data = message.getBytes();
            socket.send(new DatagramPacket(data, data.length, addr, serverPort));

            byte[] buf = new byte[1024];
            DatagramPacket resp = new DatagramPacket(buf, buf.length);
            try {
                socket.receive(resp);
                return new String(resp.getData(), 0, resp.getLength());
            } catch (SocketTimeoutException e) {
                return "ERROR: No response from server (timeout)";
            }
        }
    }
}
