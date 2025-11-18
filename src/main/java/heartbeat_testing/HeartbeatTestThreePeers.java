package heartbeat_testing;

import com.P2PBRS.server.ServerMain;

import java.nio.file.Files;
import java.nio.file.Path;

import com.P2PBRS.peer.PeerMain;

public class HeartbeatTestThreePeers {

	public static void main(String[] args) throws Exception {

		Files.deleteIfExists(Path.of("data", "registry.yaml"));
        // ============================
        // 1) Start the Server
        // ============================
        new Thread(() -> {
            try {
                System.out.println("=== Starting Server ===");
                ServerMain.main(new String[]{});
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        Thread.sleep(1000); // Small delay for server to fully launch


        // ============================
        // 2) Start Peer Alice (with Heartbeat)
        // ============================
        new Thread(() -> {
            try {
                System.out.println("=== Starting Peer Alice ===");
                PeerMain.main(new String[]{
                        "register", "Alice", "STORAGE",
                        "127.0.0.1", "6001", "7001", "1000",
                        "localhost", "5000", "2000"
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        Thread.sleep(1000);


        // ============================
        // 3) Start Peer Bob (with Heartbeat)
        // ============================
        new Thread(() -> {
            try {
                System.out.println("=== Starting Peer Bob ===");
                PeerMain.main(new String[]{
                        "register", "Bob", "STORAGE",
                        "127.0.0.1", "6002", "7002", "1000",
                        "localhost", "5000", "2000"
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        Thread.sleep(1000);


        // ============================
        // 4) Start Peer Carol (NO HEARTBEAT)
        // ============================
        new Thread(() -> {
            try {
                System.out.println("=== Starting Peer Carol (NO HEARTBEAT) ===");
                PeerMain.main( new String[]{
                        "Carol", "STORAGE",
                        "127.0.0.1", "6003", "7003", "1000",
                        "localhost", "5000", "2000"
                });
                System.out.println("Carol registered WITHOUT heartbeat.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();


        // ============================
        // 5) Let the test run
        // ============================
        System.out.println("\n=== TEST_STARTED: Alice & Bob OK, Carol should time out ===\n");

        Thread.sleep(60000); // 1 minute of stability test
        System.out.println("\n=== TEST FINISHED ===\n");
    }
}

