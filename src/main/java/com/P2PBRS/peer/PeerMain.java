package com.P2PBRS.peer;

import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Scanner;
import java.util.zip.CRC32;

import org.jline.reader.*;
import org.jline.reader.impl.DefaultParser;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

public class PeerMain {
    private static int request = 0;

    public static void main(String[] args) throws Exception {
        if (args.length == 0 || !"register".equals(args[0])) {
            helpStartup();
            System.exit(1);
        }

        if (args.length < 7) {
            helpStartup();
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
        Integer timeoutOpt = args.length > 9 ? Integer.parseInt(args[9]) : null;

        PeerNode self = new PeerNode(name, role, ip, udpPort, tcpPort, storage);

        // IMPORTANT: bind UDP client to the registered udpPort
        UDPClient client = new UDPClient(host, port, udpPort);
        if (timeoutOpt != null) client.setTimeout(timeoutOpt);

        String regResp = client.sendRegister(request++, self);
        System.out.println("Server Response: " + regResp);
        if (!regResp.startsWith("REGISTERED")) {
            System.err.println("Registration failed, exiting.");
            client.close();
            System.exit(2);
        }

        // Always de-register on JVM exit; uses the same bound socket
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                String resp = client.sendDeregister(request++, name);
                System.out.println("[shutdown] Server Response: " + resp);
            } catch (Exception e) {
                System.err.println("[shutdown] Failed to de-register: " + e.getMessage());
            } finally {
                client.close();
            }
        }));

        // Interactive CLI
        System.out.println("\n== Peer CLI (registered as " + name + ", role " + role + ") ==");
        printHelpInCli();

        Terminal terminal = TerminalBuilder.builder()
            .system(true)
            .build();

        LineReader reader = LineReaderBuilder.builder()
            .terminal(terminal)
            .parser(new DefaultParser())
            .variable(LineReader.HISTORY_FILE, java.nio.file.Paths.get(System.getProperty("user.home"), ".peercli_history"))
            .history(new DefaultHistory())
            .build();

        while (true) {
            String line;
            try {
                line = reader.readLine("> ");  // handles arrow keys and editing
            } catch (UserInterruptException | EndOfFileException e) {
                break; // Ctrl-C or Ctrl-D exits cleanly
            }

            line = line.trim();
            if (line.isEmpty()) continue;

            String[] toks = line.split("\\s+");
            String cmd = toks[0].toLowerCase();

            String resp;

            try {
                switch (cmd) {
                    case "help":
                        printHelpInCli();
                        break;
                    case "backup":
                        // backup <FilePath> <ChunkSizeBytes>
                        if (toks.length < 3) {
                            System.out.println("usage: backup <FilePath> <ChunkSizeBytes>");
                            break;
                        }
                        Path filePath = Path.of(toks[1]);
                        int chunkSize = Integer.parseInt(toks[2]);

                        if (!Files.exists(filePath) || !Files.isRegularFile(filePath)) {
                            System.out.println("ERROR: file not found: " + filePath);
                            break;
                        }
                        long fileSize = Files.size(filePath);
                        String fileName = filePath.getFileName().toString();

                        CRC32 crc = new CRC32();
                        try (FileInputStream fis = new FileInputStream(filePath.toFile())) {
                            byte[] buf = new byte[64 * 1024];
                            int n;
                            while ((n = fis.read(buf)) > 0) crc.update(buf, 0, n);
                        }
                        String checksumHex = Long.toHexString(crc.getValue());

                        resp = client.sendBackupReq(request++, fileName, fileSize, checksumHex, chunkSize);
                        System.out.println("Server Response: " + resp);
                        // TODO: Parse BACKUP_PLAN and send chunks via TCP to each storage peer.
                        resp = client.sendBackupDone(request++, fileName);
                        System.out.println("Server Response: " + resp);
                        break;

                    case "deregister":
                    case "exit":
                    case "quit":
                        // triggers shutdown hook for deregister
                        return;

                    default:
                        System.out.println("Unknown command: " + cmd);
                        printHelpInCli();
                }
            } catch (Exception e) {
                System.err.println("Command failed: " + e.getMessage());
            }
        }
    }

    public static void helpStartup() {
    System.out.println(
        "Startup usage:\n" +
        "  register <Name> <OWNER|STORAGE|BOTH> <IP_Address> <UDP_Port> <TCP_Port> <Capacity Bytes> [<ServerHost>] [<ServerPort>] [<timeout ms>]\n" +
        "\n" +
        "Example:\n" +
        "  register Alice BOTH 192.168.1.10 5001 6001 104857600 localhost 5000 2000\n"
    );
}

private static void printHelpInCli() {
    System.out.println(
        "Commands:\n" +
        "  backup <FilePath> <ChunkSizeBytes>   # announce backup to server\n" +
        "  deregister                           # de-register now and exit\n" +
        "  help                                 # show this\n" +
        "  exit | quit                          # exit (auto de-register)\n"
    );
}
}
