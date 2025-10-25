package com.p2pbrs.server;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import networking.udpBaseClient;

public class UDPServer {
    private int port;
    
    public UDPServer(int port) {
        this.port = port;
    }
    
    public void start() {
        System.out.println("Server started on port: " + port);
        // TODO: Implement UDP socket
    }
}