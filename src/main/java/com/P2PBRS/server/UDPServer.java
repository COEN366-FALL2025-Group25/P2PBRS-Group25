package com.P2PBRS.server;

import java.net.DatagramSocket;
import java.net.SocketException;
import java.net.DatagramPacket;
import java.io.IOException;

public class UDPServer {
    private int port;
    private DatagramSocket socket;

    public UDPServer(int port) {
        this.port = port;
    }

    public void start() {
        try {
            socket = new DatagramSocket(port);
            System.out.println("UDP Server is running on port " + port);
            System.out.println("Waiting for incoming packets...");

            while (true) {
                byte[] buffer = new byte[1024];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);

                new ClientHandler(packet, socket).start();
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        }
    }
}
