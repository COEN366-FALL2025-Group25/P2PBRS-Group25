package com.p2pbrs.server;

import java.net.DatagramSocket;
import java.net.SocketException;

public class UDPServer {
    private int port;
    
    public UDPServer(int port) {
        this.port = port;
    }
    
    public void start() {
        try {
            DatagramSocket socket = new DatagramSocket(port);
            System.out.println("UDP Server is running on port " + port);
            System.out.println("Waiting for incoming packets...");

            while (true) {
                Thread.sleep(1000); // Temporary placeholder
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}