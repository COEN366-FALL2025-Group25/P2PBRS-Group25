package com.P2PBRS.peer;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class HeartbeatSender extends Thread {

	private final UDPClient client;
	private final PeerNode self;
	private volatile boolean running = true; // We need volatile to ensure threads see the same value

	public HeartbeatSender(UDPClient client, PeerNode self) {
		this.client = client;
		this.self = self;
	}

	// To stop the heartbeat
	public void stopHeartbeat() {
		running = false;
		this.interrupt(); // If the Thread is sleeping it wakes it up to break
	}

	@Override
	public void run() {

		while (running) {
			try {
				Thread.sleep(2500);

				int request = PeerMain.nextRequest();
				// Send heartbeat
				String reply = client.sendHeartbeat(request, self);

				if (reply.contains("HEARTBEAT " + request + " ERROR Client not found")) {
					System.out.println("Disconnected from server. Shutting HEARTBEAT down");
					stopHeartbeat();
				}

				// System.out.println("Heartbeat: " + request + "\n " + "Server answered: " +
				// reply); OPTIONAL

				// Leave 5 seconds between heartbeats
				Thread.sleep(2500);

			} catch (TimeoutException e) {
				System.out.println("Timeout, no response from server");
			} catch (InterruptedException e) {
				System.out.println("Interrupted Thread");
				running = false; // We need to exit the loop
			} catch (ExecutionException | IOException e) {
				if (running) { // prevent error after socket is closed
					System.err.println("Error while sending heartbeat: " + e.getMessage());
					// e.printStackTrace(); // optional: comment to avoid full stack trace on
					// shutdown
				}
			}
		}

		System.out.println("HeartbeatSender stopped");
	}
}
