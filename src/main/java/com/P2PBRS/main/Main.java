package com.P2PBRS.main;

import com.P2PBRS.peer.PeerMain;
import com.P2PBRS.server.ServerMain;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Main {

    public static void main(String[] args) {
        System.out.println("Starting P2PBRS...");

        boolean help = false;

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-h"))
            {
                help = true;
            }
        }

        try {
            if (args.length == 0) {
                printTopLevelUsage();
                System.exit(1);
            }
            String mode = args[0].toLowerCase();

            switch (mode) {
                case "-h":
                case "--help":
                    printTopLevelUsage();
                    ServerMain.help();
                    PeerMain.help();
                    break;
                case "server":
                    try {
                        if (help) {
                            ServerMain.help();
                        } else {
                            ServerMain.main(Arrays.copyOfRange(args, 1, args.length));
                        }
                    }
                    catch(Exception e) {
                        System.out.println("Server failed: " + e);
                        System.exit(1);
                    }
                    break;
                case "peer":
                    try {
                        if (help) {
                            PeerMain.help();
                        } else {
                            PeerMain.main(Arrays.copyOfRange(args, 1, args.length));
                        }
                    }
                    catch(Exception e) {
                        System.out.println("Peer failed: " + e);
                        System.exit(1);
                    }
                    break;
                default:
                    System.err.println("Unknown mode: " + mode);
                    printTopLevelUsage();
                    System.exit(1);
            }
        }
        catch(Exception e) {
            printTopLevelUsage();
            System.exit(1);
        }
    }

    private static void printTopLevelUsage() {
        StringBuilder sb = new StringBuilder();
        sb.append("Usage:\n");
        sb.append("  server [server-args...]\n");
        sb.append("  peer   [peer-args...]\n");
        System.out.println(sb.toString());
    }
}
