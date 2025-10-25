package com.p2pbrs.server;

public class ServerMain {
    public static void main(String[] args) {
        System.out.println("P2PBRS Server Starting...");
        
        UDPServer server = new UDPServer(5000);
        server.start();
    }
}