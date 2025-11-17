package com.P2PBRS.server;

import com.P2PBRS.server.UDPServer;

public class ServerMain {
    public static void main(String[] args) {
        System.out.println("P2PBRS Server Starting...");

        UDPServer server = new UDPServer(5000);
        server.start();
        new HeartbeatHandler().start();
    }

    public static void help() {
        StringBuilder sb = new StringBuilder();
        sb.append("Server args:\n");
        sb.append("  [<port>]\n");
        sb.append("Examples:\n");
        sb.append("  # run server\n");
        sb.append("  5000\n");
        System.out.println(sb.toString());
    }
}
