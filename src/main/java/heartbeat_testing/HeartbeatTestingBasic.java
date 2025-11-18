package heartbeat_testing;

import com.P2PBRS.server.ServerMain;

import java.nio.file.Files;
import java.nio.file.Path;

import com.P2PBRS.peer.PeerMain;

public class HeartbeatTestingBasic {

	public static void main(String[] args) throws Exception {
		
		Files.deleteIfExists(Path.of("data", "registry.yaml"));

		// 1) Start the server in background
		Thread serverThread = new Thread(() -> {
			try {
				System.out.println("=== Starting Server ===");
				ServerMain.main(new String[] {}); // starts default port 5000
			} catch (Exception e) {
				e.printStackTrace();
			}
		}, "Test-Server-Thread");

		// IMPORTANT: daemon so JVM exits automatically when clients finish
		serverThread.setDaemon(true);
		serverThread.start();

		// Small delay to let server start up
		Thread.sleep(1000);

		// 2) Start a storage peer that will send heartbeats
		Thread clientThread = new Thread(() -> {
			try {
				System.out.println("=== Starting Peer Alice ===");

				String[] peerArgs = { "register", // command
						"Alice", // name
						"STORAGE", // role
						"127.0.0.1", // IP (must match what server sees)
						"6001", // UDP port
						"7001", // TCP port
						"1000" // storage capacity
				};

				PeerMain.main(peerArgs);

			} catch (Exception e) {
				e.printStackTrace();
			}
		}, "Test-Client-Thread");

		clientThread.start();

		// 3) Prevent main from exiting immediately
		clientThread.join(); // wait for peer CLI to finish
	}
}
