package com.P2PBRS.peer;

public class PeerMain {
    private static int request = 0;

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            help();
            System.exit(1);
        }

        switch (args[0]) {
            case "register": {
                if (args.length < 7) {
                    help();
                    System.exit(1);
                }
                String name = args[1];
                String role = args[2];
                String ip = args[3];
                int udpPort = Integer.parseInt(args[4]);
                int tcpPort = Integer.parseInt(args[5]);
                int storage = Integer.parseInt(args[6]);
                String host = args.length > 7 ? args[7] : "localhost";
                int port = args.length > 8 ? Integer.parseInt(args[8]) : 5000;

                PeerNode node = new PeerNode(name, role, ip, udpPort, tcpPort, storage);
                UDPClient client = new UDPClient(host, port);
                if (args.length > 9) client.setTimeout(Integer.parseInt(args[9]));

                String response = client.sendRegister(request++, node);
                System.out.println("Server Response: " + response);
                break;
            }

            case "deregister": {
                if (args.length < 2) {
                    help();
                    System.exit(1);
                }
                String name = args[1];
                String host = args.length > 2 ? args[2] : "localhost";
                int port = args.length > 3 ? Integer.parseInt(args[3]) : 5000;

                UDPClient client = new UDPClient(host, port);
                if (args.length > 4) client.setTimeout(Integer.parseInt(args[4]));

                String response = client.sendDeregister(request++, name);
                System.out.println("Server Response: " + response);
                break;
            }

            default:
                System.err.println("Unknown action: " + args[0]);
                help();
                System.exit(1);
        }
    }

    public static void help() {
        StringBuilder sb = new StringBuilder();
        sb.append("Peer args:\n");
        sb.append("  register <Name> <OWNER|STORAGE|BOTH> <IP_Address> <UDP_Port> <TCP_Port> <Capacity Bytes> [<ServerHost>] [<ServerPort>] [<timeout ms>]\n");
        sb.append("  deregister <Name> [<ServerHost>] [<ServerPort>] [<timeout ms>]\n\n");
        sb.append("Examples:\n");
        sb.append("  register Alice BOTH 192.168.1.10 5001 6001 1024 localhost 5000\n");
        sb.append("  deregister Alice localhost 5000\n");
        System.out.println(sb.toString());
    }
}
