package com.P2PBRS.peer;

import java.io.Closeable;
import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.Optional;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

public class UDPClient implements Closeable {
	private final String serverHost;
	private final int serverPort;
	private final int localUdpPort;
	private final DatagramSocket socket;
	private volatile int timeoutMs = 2000;

	// Receiver machinery
	private final ExecutorService rxExec;
	private volatile boolean running = true;

	// --- Pending request wrapper with matcher ---
	private static final class PendingRequest {
		final CompletableFuture<String> future = new CompletableFuture<>();
		final Predicate<String> matcher;

		PendingRequest(Predicate<String> matcher) {
			this.matcher = matcher;
		}
	}

	// Pending requests keyed by RQ#
	private final ConcurrentMap<Integer, PendingRequest> pending = new ConcurrentHashMap<>();

	// Handler for unsolicited messages (message, sender)
	private volatile BiConsumer<String, SocketAddress> unsolicitedHandler = (msg, from) -> System.out
			.println("[unsolicited] " + from + " -> " + msg);

	public UDPClient(String serverHost, int serverPort, int localUdpPort) throws SocketException {
		this.serverHost = Objects.requireNonNull(serverHost);
		this.serverPort = serverPort;
		this.localUdpPort = localUdpPort;

		this.socket = new DatagramSocket(new InetSocketAddress(localUdpPort));
		this.socket.setReuseAddress(true);
		this.socket.setSoTimeout(/* read timeout used only during close */ 1000);

		// Single background thread to receive and dispatch
		this.rxExec = Executors.newSingleThreadExecutor(r -> {
			Thread t = new Thread(r, "UDPClient-Rx");
			t.setDaemon(true);
			return t;
		});
		rxExec.execute(this::receiveLoop);
	}

	/** Optional: set the timeout used by sendCommand waits (ms). */
	public UDPClient setTimeout(int timeoutMs) {
		this.timeoutMs = timeoutMs;
		return this;
	}

	/**
	 * Install a callback for unsolicited packets (not matching a pending request).
	 */
	public void setUnsolicitedHandler(BiConsumer<String, SocketAddress> handler) {
		this.unsolicitedHandler = (handler == null)
				? (msg, from) -> System.out.println("[unsolicited] " + from + " -> " + msg)
				: handler;
	}

	public String sendRegister(int rqNumber, PeerNode node)
			throws IOException, TimeoutException, ExecutionException, InterruptedException {
		// REGISTER RQ# Name Role IP_Address UDP_Port# TCP_Port# Storage_Capacity
		return sendCommand(rqNumber, timeoutMs, defaultRqMatcher(rqNumber), "REGISTER", node.getName(), node.getRole(),
				node.getIpAddress(), String.valueOf(node.getUdpPort()), String.valueOf(node.getTcpPort()),
				String.valueOf(node.getStorageCapacity()));
	}

	public String sendDeregister(int rqNumber, String name)
			throws IOException, TimeoutException, ExecutionException, InterruptedException {
		// DE-REGISTER RQ# Name
		return sendCommand(rqNumber, timeoutMs, defaultRqMatcher(rqNumber), "DE-REGISTER", name);
	}

	public String sendBackupReq(int rqNumber, String fileName, long fileSize, String checksumHex, int chunkSize)
			throws IOException, TimeoutException, ExecutionException, InterruptedException {
		// BACKUP_REQ RQ# File_Name File_Size Checksum Chunk_Size
		return sendCommand(rqNumber, timeoutMs, defaultRqMatcher(rqNumber), "BACKUP_REQ", fileName,
				String.valueOf(fileSize), checksumHex, String.valueOf(chunkSize));
	}

	public String sendBackupDone(int rqNumber, String fileName)
			throws IOException, TimeoutException, ExecutionException, InterruptedException {
		// BACKUP_DONE RQ# File_Name
		return sendCommand(rqNumber, timeoutMs, defaultRqMatcher(rqNumber), "BACKUP_DONE", fileName);
	}

	public String sendRestoreReq(int rqNumber, String fileName)
			throws IOException, TimeoutException, ExecutionException, InterruptedException {
		// RESTORE_REQ RQ# File_Name
		return sendCommand(rqNumber, timeoutMs, defaultRqMatcher(rqNumber), "RESTORE_REQ", fileName);
	}

	public String sendHeartbeat(int rqNumber, PeerNode node)
			throws IOException, TimeoutException, ExecutionException, InterruptedException {
		String timestamp = LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
		return sendCommand(rqNumber, timeoutMs, defaultRqMatcher(rqNumber), "HEARTBEAT", node.getName(),
				String.valueOf(node.getNumberChunksStored()), timestamp);
	}

	// Default matcher: reply must carry the same RQ#
	private Predicate<String> defaultRqMatcher(int rqNumber) {
		return msg -> extractRq(msg).map(rq -> rq == rqNumber).orElse(false);
	}

	private String sendCommand(int rqNumber, int perCallTimeoutMs, Predicate<String> responseMatcher, String command,
			String... args) throws IOException, TimeoutException, ExecutionException, InterruptedException {

		Objects.requireNonNull(responseMatcher, "responseMatcher");

		// Register pending BEFORE sending to avoid race with fast reply
		PendingRequest req = new PendingRequest(responseMatcher);
		PendingRequest prev = pending.putIfAbsent(rqNumber, req);
		if (prev != null) {
			throw new IllegalStateException("Duplicate pending RQ#: " + rqNumber);
		}

		try {
			String payload = buildPayload(rqNumber, command, args);
			send(payload);

			// Wait for the matching response (receiver thread will complete it)
			return req.future.get(perCallTimeoutMs, TimeUnit.MILLISECONDS);

		} catch (TimeoutException | InterruptedException | ExecutionException e) {
			// On failure, remove pending and rethrow
			pending.remove(rqNumber, req);
			throw e;
		}
	}

	private String buildPayload(int rqNumber, String command, String... args) {
		StringBuilder sb = new StringBuilder(64);
		sb.append(command).append(' ').append(rqNumber);
		if (args != null)
			for (String a : args)
				sb.append(' ').append(a == null ? "" : a);
		return sb.toString();
	}

	private void send(String payload) throws IOException {
		byte[] data = payload.getBytes(StandardCharsets.UTF_8);
		InetAddress addr = InetAddress.getByName(serverHost);
		DatagramPacket dp = new DatagramPacket(data, data.length, addr, serverPort);
		socket.send(dp);
	}

	private void receiveLoop() {
		byte[] buf = new byte[8192];
		DatagramPacket pkt = new DatagramPacket(buf, buf.length);

		while (running) {
			try {
				socket.receive(pkt);
				String msg = new String(pkt.getData(), 0, pkt.getLength(), StandardCharsets.UTF_8);

				// Extract RQ# if present: <CMD> <RQ#> ...
				Optional<Integer> rqOpt = extractRq(msg);
				boolean delivered = false;

				// 1) Fast-path: deliver by exact RQ# if we have a waiter
				if (rqOpt.isPresent()) {
					PendingRequest req = pending.get(rqOpt.get());
					if (req != null) {
						// Ensure the matcher also agrees (extra safety / preprocessing window)
						if (safeTest(req.matcher, msg)) {
							if (req.future.complete(msg)) {
								pending.remove(rqOpt.get(), req);
								delivered = true;
							}
						}
					}
				}

				// 2) Fallback: scan all matchers (covers atypical replies or handler-claimed
				// responses)
				if (!delivered && !pending.isEmpty()) {
					for (Map.Entry<Integer, PendingRequest> e : pending.entrySet()) {
						PendingRequest pr = e.getValue();
						if (safeTest(pr.matcher, msg)) {
							if (pr.future.complete(msg)) {
								pending.remove(e.getKey(), pr);
								delivered = true;
								break;
							}
						}
					}
				}

				// 3) If nobody claimed it, treat as unsolicited/control
				if (!delivered) {
					BiConsumer<String, SocketAddress> cb = this.unsolicitedHandler;
					if (cb != null)
						cb.accept(msg, pkt.getSocketAddress());
				}

			} catch (SocketTimeoutException ignored) {
				// short timeout only to allow graceful close; loop continues
			} catch (SocketException se) {
				if (running)
					System.err.println("[udp-rx] Socket error: " + se.getMessage());
				break;
			} catch (Exception e) {
				System.err.println("[udp-rx] Receive/dispatch failed: " + e.getMessage());
			}
		}
	}

	private boolean safeTest(Predicate<String> matcher, String msg) {
		try {
			return matcher == null || matcher.test(msg);
		} catch (Throwable t) {
			return false;
		}
	}

	private Optional<Integer> extractRq(String msg) {
		// Expect "TOKEN RQ# ..." where RQ# is a decimal int
		int firstSpace = msg.indexOf(' ');
		if (firstSpace < 0)
			return Optional.empty();
		int secondSpace = msg.indexOf(' ', firstSpace + 1);
		if (secondSpace < 0)
			secondSpace = msg.length();
		String rqToken = msg.substring(firstSpace + 1, secondSpace).trim();
		try {
			return Optional.of(Integer.parseInt(rqToken));
		} catch (NumberFormatException e) {
			return Optional.empty();
		}
	}

	@Override
	public void close() {
		running = false;
		try {
			socket.setSoTimeout(200);
		} catch (Exception ignored) {
		}
		try {
			socket.close();
		} catch (Exception ignored) {
		}
		rxExec.shutdownNow();
		pending.forEach((rq, pr) -> pr.future.completeExceptionally(new CancellationException("Client closed")));
		pending.clear();
	}

	public byte[] receiveChunk(SocketAddress from, int chunkSize) throws IOException {
		byte[] buf = new byte[chunkSize];
		DatagramPacket pkt = new DatagramPacket(buf, buf.length);
		while (true) {
			socket.receive(pkt);
			if (pkt.getSocketAddress().equals(from)) {
				return Arrays.copyOf(pkt.getData(), pkt.getLength());
			}
		}
	}

	public void sendChunk(String storageIp, int storagePort, byte[] chunk) throws IOException {
		InetAddress addr = InetAddress.getByName(storageIp);
		DatagramPacket pkt = new DatagramPacket(chunk, chunk.length, addr, storagePort);
		socket.send(pkt);
	}

	public String sendRestoreDone(int rqNumber, String fileName)
			throws IOException, TimeoutException, ExecutionException, InterruptedException {
		return sendCommand(rqNumber, timeoutMs, defaultRqMatcher(rqNumber), "RESTORE_OK", fileName);
	}

	public String sendRestoreFailed(int rqNumber, String fileName, String reason)
			throws IOException, TimeoutException, ExecutionException, InterruptedException {
		return sendCommand(rqNumber, timeoutMs, defaultRqMatcher(rqNumber), "RESTORE_FAIL", fileName, reason);
	}

	public String sendReplicateReq(int rqNumber, String fileName, int chunkId, String targetPeer) 
			throws IOException, TimeoutException, ExecutionException, InterruptedException {
		// REPLICATE_REQ RQ# File_Name Chunk_ID Target_Peer
		return sendCommand(rqNumber, timeoutMs, defaultRqMatcher(rqNumber), "REPLICATE_REQ", fileName, String.valueOf(chunkId), targetPeer);
	}

	public String sendReplicateDone(int requestId, String fileName, int chunkId, String targetPeer) 
		throws TimeoutException, ExecutionException, InterruptedException, IOException {
		// REPLICATE_DONE RQ# File_Name Chunk_ID Target_Peer
		return sendCommand(requestId, timeoutMs, defaultRqMatcher(requestId), 
			"REPLICATE_DONE", fileName, String.valueOf(chunkId), targetPeer);
	}

	public void sendMessage(String message) throws IOException {
		byte[] data = message.getBytes(StandardCharsets.UTF_8);
		InetAddress addr = InetAddress.getByName(serverHost);
		DatagramPacket dp = new DatagramPacket(data, data.length, addr, serverPort);
		socket.send(dp);
	}
}
