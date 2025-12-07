package com.P2PBRS.peer;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.zip.CRC32;
import java.util.concurrent.TimeoutException;

import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.DefaultParser;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

public class PeerMain {
	private static int request = 0;
	private static final Map<String, String> storagePeerIps = new ConcurrentHashMap<>(); // maps storage peer name -> IP
	private static final Map<String, Integer> storagePeerPorts = new ConcurrentHashMap<>(); // maps storage peer name ->
																							// Port
	private static final Map<String, Long> fileChecksums = new ConcurrentHashMap<>();

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
		if (timeoutOpt != null)
			client.setTimeout(timeoutOpt);

		Path storageDir = null;
		if ("STORAGE".equalsIgnoreCase(role) || "BOTH".equalsIgnoreCase(role)) {
			storageDir = Path.of("storage_" + name);
			Files.createDirectories(storageDir); // make sure folder exists
		}

		if (storageDir != null) {
			Path finalStorageDir = storageDir; // for lambda capture
			client.setUnsolicitedHandler((msg, from) -> {
				try {
					if (msg.startsWith("STORE_REQ")) {
						// Expected format: STORE_REQ <RQ#> <FileName> <ChunkID> <OwnerPeer>
						String[] parts = msg.split("\\s+");
						System.out.println("DEBUG STORE_REQ: " + Arrays.toString(parts));

						if (parts.length < 5) {
							System.err.println("Malformed STORE_REQ: " + msg);
							return;
						}

						String rq = parts[1];
						String fileName = parts[2];
						int chunkId = Integer.parseInt(parts[3]);
						String ownerPeer = parts[4];

						System.out.println("STORE_REQ: Ready for chunk " + chunkId + " of " + fileName + " from " + ownerPeer); // The actual chunk will  come via TCP from the owner
					} else if (msg.startsWith("STORAGE_TASK")) {
						// Expected format: STORAGE_TASK <RQ#> <FileName> <ChunkSize> <OwnerPeer>
						String[] parts = msg.split("\\s+");
						System.out.println("DEBUG STORAGE_TASK: " + Arrays.toString(parts));

						if (parts.length < 5) {
							System.err.println("Malformed STORAGE_TASK: " + msg);
							return;
						}

						String rq = parts[1];
						String fileName = parts[2];
						int chunkSize = Integer.parseInt(parts[3]);
						String ownerPeer = parts[4];

						System.out.println("STORAGE_TASK: Will receive " + fileName + " with chunk size " + chunkSize
								+ " from " + ownerPeer);

						System.out.println("Note: Owner peer " + ownerPeer + " may need to be added to peer maps");
					} else if (msg.startsWith("REPLICATE_REQ")) {
						System.out.println("STORAGE PEER: Received REPLICATE_REQ");
						System.out.println("Full message: " + msg);

						// Parse the REPLICATE_REQ to extract target peer info
						String[] parts = msg.split("\\s+");
						if (parts.length >= 5) {
							String rq = parts[1];
							String fileName = parts[2];
							int chunkId = Integer.parseInt(parts[3]);
							String targetPeer = parts[4];
							
							System.out.println("Will replicate " + fileName + " chunk " + chunkId + " to " + targetPeer);
						}
						
						processReplicateReq(client, msg, finalStorageDir);
					} else if (msg.startsWith("PEER_INFO")) {
						// PEER_INFO <PeerName> <IP_Address> <TCP_Port>
						String[] parts = msg.split("\\s+");
						if (parts.length < 4) {
							System.err.println("Malformed PEER_INFO: " + msg);
							return;
						}
						String peerName = parts[1];
						String peerIp = parts[2];
						int peerTcpPort = Integer.parseInt(parts[3]);

						storagePeerIps.put(peerName, peerIp);
						storagePeerPorts.put(peerName, peerTcpPort);

						System.out.println("Updated storage peer map from PEER_INFO: " + peerName + " -> " + peerIp + ":" + peerTcpPort);
					
					} else if (msg.startsWith("PEER_REMOVED")) {
						// PEER_REMOVED <PeerName>
						String[] parts = msg.split("\\s+");
						if (parts.length >= 2) {
							String removedPeer = parts[1];
							storagePeerIps.remove(removedPeer);
							storagePeerPorts.remove(removedPeer);
							System.out.println("Removed peer from maps: " + removedPeer);
							System.out.println("Updated peer maps:");
							System.out.println("  IPs: " + storagePeerIps);
							System.out.println("  Ports: " + storagePeerPorts);
						}
					} else {
						System.out.println("[unsolicited] " + from + " -> " + msg);
					} 
				} catch (Exception e) {
					System.err.println("Failed to handle unsolicited message: " + e.getMessage());
					e.printStackTrace();
				}
			});

			// Start TCP server for incoming chunk storage requests
			new Thread(() -> {
				try (ServerSocket serverSocket = new ServerSocket(tcpPort)) {
					System.out.println("TCP storage server listening on port " + tcpPort);
					while (true) {
						Socket socket = serverSocket.accept();
						new Thread(() -> handleIncomingChunk(socket, finalStorageDir, self)).start();
					}
				} catch (Exception e) {
					System.err.println("TCP server failed: " + e.getMessage());
				}
			}, "Storage-TCP-Server").start();
		}

		String regResp = client.sendRegister(PeerMain.nextRequest(), self);
		System.out.println("Server Response: " + regResp);
		if (!regResp.startsWith("REGISTERED")) {
			System.err.println("Registration failed, exiting.");
			client.close();
			System.exit(2);
		}

		// Register self in peer maps for replication
		storagePeerIps.put(self.getName(), self.getIpAddress());
		storagePeerPorts.put(self.getName(), self.getTcpPort());
		System.out.println("Added self to storage peer maps for replication: " + self.getName() + " -> "
				+ self.getIpAddress() + ":" + self.getTcpPort());

		// Start Heartbeat after registering
		new HeartbeatSender(client, self).start();
		System.out.println("Heartbeat started");

		// Interactive CLI
		System.out.println("\n== Peer CLI (registered as " + name + ", role " + role + ") ==");
		printHelpInCli();

		Terminal terminal = TerminalBuilder.builder().system(true).build();

		LineReader reader = LineReaderBuilder.builder().terminal(terminal).parser(new DefaultParser())
				.variable(LineReader.HISTORY_FILE,
						java.nio.file.Paths.get(System.getProperty("user.home"), ".peercli_history"))
				.history(new DefaultHistory()).build();

		while (true) {
			String line;
			try {
				line = reader.readLine("> "); // handles arrow keys and editing
			} catch (UserInterruptException | EndOfFileException e) {
				break; // Ctrl-C or Ctrl-D exits cleanly
			}

			line = line.trim();
			if (line.isEmpty())
				continue;

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
						while ((n = fis.read(buf)) > 0)
							crc.update(buf, 0, n);
					}
					long checksumValue = crc.getValue();
					String checksumHex = Long.toHexString(checksumValue);

					// Remember this checksum locally so we can verify the restored file later
					fileChecksums.put(fileName, checksumValue);

					resp = client.sendBackupReq(PeerMain.nextRequest(), fileName, fileSize, checksumHex, chunkSize);
					System.out.println("Server Response: " + resp);

					// Parse BACKUP_PLAN with connection details
					List<String> assignedPeers = new ArrayList<>();
					int planChunkSize = chunkSize;

					if (resp.startsWith("BACKUP_PLAN")) {
						System.out.println("BACKUP_PLAN parsing: " + resp);

						int startBracket = resp.indexOf('[');
						int endBracket = resp.indexOf(']');
						if (startBracket >= 0 && endBracket > startBracket) {
							String peersStr = resp.substring(startBracket + 1, endBracket);
							System.out.println("  - Raw peer string: '" + peersStr + "'");

							String[] peerEntries = peersStr.split(",");

							for (String peerEntry : peerEntries) {
								peerEntry = peerEntry.trim();
								System.out.println("  - Parsing peer entry: '" + peerEntry + "'");

								String[] parts = peerEntry.split(":");

								if (parts.length == 3) {
									// Format: PeerName:IP:TCPPort
									String peerName = parts[0];
									String peerIp = parts[1];
									int peerTcpPort = Integer.parseInt(parts[2]);

									assignedPeers.add(peerName);
									storagePeerIps.put(peerName, peerIp);
									storagePeerPorts.put(peerName, peerTcpPort);

									System.out.println("  - Mapped " + peerName + " -> " + peerIp + ":" + peerTcpPort
											+ " (from server)");
								} else {
									// Fallback for unexpected format
									System.err.println("  - WARNING: Unexpected peer format: " + peerEntry);
									String peerName = peerEntry;
									assignedPeers.add(peerName);
									// Don't set fallback - we want to see the error clearly
								}
							}
						} else {
							System.err.println("  - ERROR: Could not find peer list in brackets");
						}

						// Parse chunk size
						String afterBracket = resp.substring(endBracket + 1).trim();
						String[] tokens = afterBracket.split("\\s+");
						if (tokens.length > 0) {
							try {
								planChunkSize = Integer.parseInt(tokens[tokens.length - 1]);
								System.out.println("  - Chunk size: " + planChunkSize);
							} catch (NumberFormatException e) {
								System.err.println("Warning: Could not parse chunk size from BACKUP_PLAN");
							}
						}
					}

					// Validate we have connection info for all peers
					for (String peerName : assignedPeers) {
						if (!storagePeerIps.containsKey(peerName) || !storagePeerPorts.containsKey(peerName)) {
							System.err.println("ERROR: Missing connection info for peer: " + peerName);
							System.err.println("Cannot proceed with backup - no IP/port mapping");
							return;
						}
					}

					System.out.println("BACKUP_PLAN parsed:");
					System.out.println("  - File: " + fileName);
					System.out.println("  - Assigned peers: " + assignedPeers);
					System.out.println("  - Chunk size: " + planChunkSize);

					// Send ALL chunks to the assigned storage peer(s)
					// For single peer assignment, send all chunks to that one peer
					// For multiple peers, distribute chunks among them

					try (FileInputStream fis = new FileInputStream(filePath.toFile())) {
						byte[] buffer = new byte[planChunkSize];
						int bytesRead;
						int chunkId = 0;

						while ((bytesRead = fis.read(buffer)) > 0) {
							byte[] chunk = Arrays.copyOf(buffer, bytesRead);
							System.out.println("Prepared chunk " + chunkId + " of size " + bytesRead);

							// Determine which peer should get this chunk
							String peerName = assignedPeers.get(chunkId % assignedPeers.size());
							String peerIp = storagePeerIps.get(peerName);
							int peerPort = storagePeerPorts.get(peerName);

							boolean sent = false;
							int attempts = 0;
							while (!sent && attempts < 3) {
								attempts++;
								try (Socket tcpSocket = new Socket(peerIp, peerPort);
										OutputStream out = tcpSocket.getOutputStream();
										InputStream in = tcpSocket.getInputStream();
										Scanner responseScanner = new Scanner(in, StandardCharsets.UTF_8.name())) {

									// Calculate checksum for THIS CHUNK
									CRC32 chunkCrc = new CRC32();
									chunkCrc.update(chunk);
									String chunkChecksumHex = Long.toHexString(chunkCrc.getValue());

									// Send metadata header
									String header = request + " " + fileName + " " + chunkId + " " + chunk.length + " "
											+ chunkChecksumHex + "\n";
									out.write(header.getBytes(StandardCharsets.UTF_8));
									out.write(chunk);
									out.flush();

									System.out.println("Sent chunk " + chunkId + " to " + peerName + " at " + peerIp
											+ ":" + peerPort);

									// Wait for acknowledgment
									tcpSocket.setSoTimeout(5000);
									if (responseScanner.hasNextLine()) {
										String ack = responseScanner.nextLine();
										if (ack.startsWith("CHUNK_OK")) {
											System.out.println("Received acknowledgment: " + ack);
											sent = true;
										} else {
											System.err.println("Unexpected response: " + ack);
										}
									} else {
										System.err.println("No acknowledgment received from " + peerName);
									}

								} catch (SocketTimeoutException e) {
									System.err.println("Timeout waiting for acknowledgment from " + peerName);
								} catch (Exception e) {
									System.err.println("Failed to send chunk " + chunkId + " to " + peerName
											+ " attempt " + attempts + ": " + e.getMessage());
								}
							}

							if (!sent) {
								System.err.println("Giving up on chunk " + chunkId + " for peer " + peerName);
								break; // Stop the backup if a chunk fails
							}

							chunkId++;
						}

						System.out.println("Successfully sent " + chunkId + " chunks for file " + fileName);
					}

					resp = client.sendBackupDone(PeerMain.nextRequest(), fileName);
					System.out.println("Server Response: " + resp);
					break;
				case "restore":
					if (toks.length < 2) {
						System.out.println("usage: restore <FileName>");
						break;
					}
					fileName = toks[1];
					resp = client.sendRestoreReq(PeerMain.nextRequest(), fileName);
					System.out.println("Server Response: " + resp);

					if (resp.startsWith("RESTORE_PLAN")) {
						System.out.println("Parsing restore plan");

						// Parse: RESTORE_PLAN <RQ#> <FileName> [Peer1:IP:Port,Peer2:IP:Port,...] <ChunkSize> <TotalChunks> <FileChecksum>

						int start = resp.indexOf("[");
						int end = resp.indexOf("]");
						if (start < 0 || end < 0) {
							System.err.println("Malformed RESTORE_PLAN (missing brackets)");
							break;
						}

						String peersStr = resp.substring(start + 1, end);
						List<String> restorePeers = new ArrayList<>();

						for (String peerEntry : peersStr.split(",")) {
							String[] parts = peerEntry.trim().split(":");
							if (parts.length == 3) {
								storagePeerIps.put(parts[0], parts[1]);
								storagePeerPorts.put(parts[0], Integer.parseInt(parts[2]));
								restorePeers.add(String.join(":", parts)); // Store full format
							} else {
								System.err.println("Skipping malformed peer entry: " + peerEntry);
							}
						}

						// Extract chunk size, total chunks, and checksum
						String afterBracket = resp.substring(end + 1).trim();
						String[] tokens = resp.substring(end + 1).trim().split("\\s+");

						if (tokens.length < 3) {
							System.err.println("Malformed RESTORE_PLAN (missing chunk size, total chunks, or checksum)");
							break;
						}

						chunkSize = Integer.parseInt(tokens[0]);
						int totalChunks = Integer.parseInt(tokens[1]);

						String fileChecksum = tokens.length > 2 ? tokens[2] : null;

						System.out.println("Restore details: " + chunkSize + " bytes/chunk, " + totalChunks + " total chunks, checksum "
								+ fileChecksum);

						restoreFileChunks(client, fileName, restorePeers, chunkSize, totalChunks, fileChecksum);
					}
					break;
				case "deregister":
				case "exit":
				case "quit":
					try {
						resp = client.sendDeregister(PeerMain.nextRequest(), name);
						System.out.println("[shutdown] Server Response: " + resp);
					} catch (Exception e) {
						System.err.println("[shutdown] Failed to de-register: " + e.getMessage());
					} finally {
						client.close();
					}
					return;
				case "test-replicate":
					// Manual test: send PREPLICATE_REQ message
					if (toks.length < 4) {
						System.out.println("usage: test-replicate <FileName> <ChunkID> <TargetPeer>");
						break;
					}

					fileName = toks[1];
					int chunkId = Integer.parseInt(toks[2]);
					String targetPeer = toks[3];

					System.out.println("Test sending REPLICATE_REQ for " + fileName + " chunk " + chunkId + " to " + targetPeer);
					sendReplicateReqSafe(client, fileName, chunkId, targetPeer);
					break;
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
		System.out.println("Startup usage:\n"
				+ "  register <Name> <OWNER|STORAGE|BOTH> <IP_Address> <UDP_Port> <TCP_Port> <Capacity Bytes> [<ServerHost>] [<ServerPort>] [<timeout ms>]\n"
				+ "\n" + "Example:\n" + "  register Alice BOTH 192.168.1.10 5001 6001 104857600 localhost 5000 2000\n");
	}

	private static void printHelpInCli() {
		    System.out.println("Commands:\n" + 
        "  backup <FilePath> <ChunkSizeBytes>   # announce backup to server\n" +
        "  restore <FileName>                   # restore file from backup\n" +
        "  test-replicate <File> <Chunk> <Peer> # TEST: send replicate request\n" +
        "  deregister                           # de-register now and exit\n" +
        "  help                                 # show this\n" +
        "  exit | quit                          # exit (auto de-register)\n");
	}

	private static void handleIncomingChunk(Socket socket, Path storageDir, PeerNode self) {
		String clientInfo = socket.getInetAddress() + ":" + socket.getPort();
		System.out.println("TCP connection accepted from " + clientInfo);

		try (socket; InputStream in = socket.getInputStream(); OutputStream out = socket.getOutputStream()) {

			// Read header line manually (don't use Scanner - it buffers too much)
			StringBuilder headerBuilder = new StringBuilder();
			int b;
			while ((b = in.read()) != -1) {
				char c = (char) b;
				headerBuilder.append(c);
				if (c == '\n') {
					break;
				}
			}

			String header = headerBuilder.toString().trim();
			System.out.println("Received header: " + header);

			if (header.isEmpty()) {
				System.err.println("Empty header received");
				return;
			}

			if (header.startsWith("GET_CHUNK")) {
				String[] parts = header.split("\\s+");
				if (parts.length < 4) {
					System.err.println("Malformed GET_CHUNK header: " + header);
					return;
				}

				int rq = Integer.parseInt(parts[1]);
				String fileName = parts[2];
				int chunkId = Integer.parseInt(parts[3]);

				Path chunkPath = storageDir.resolve(fileName).resolve("chunk" + chunkId);
				if (!Files.exists(chunkPath)) {
					String err = String.format("CHUNK_DATA %d %s %d ERROR\n", rq, fileName, chunkId);
					out.write(err.getBytes(StandardCharsets.UTF_8));
					out.flush();
					System.err.println("Requested chunk not found: " + chunkPath);
					return;
				}

				byte[] chunkData = Files.readAllBytes(chunkPath);

				// Compute CRC for this chunk
				CRC32 crc = new CRC32();
				crc.update(chunkData);
				long crcVal = crc.getValue();

				// Send: CHUNK_DATA RQ# File_Name Chunk_ID Checksum
				String respHeader = String.format("CHUNK_DATA %d %s %d %s\n", rq, fileName, chunkId,
						Long.toHexString(crcVal));
				out.write(respHeader.getBytes(StandardCharsets.UTF_8));
				out.write(chunkData);
				out.flush();

				System.out.println("Sent CHUNK_DATA for " + fileName + " chunk " + chunkId);
				return;
			} else if (header.startsWith("REPLICATE_CHUNK")) {
				handleReplicateChunk(header, in, out, storageDir, self);
				return;
			}

			String[] parts = header.split("\\s+");
			if (parts.length < 5) {
				System.err.println("Malformed header: " + header);
				return;
			}

			int rq = Integer.parseInt(parts[0]);
			String fileName = parts[1];
			int chunkId = Integer.parseInt(parts[2]);
			int chunkSize = Integer.parseInt(parts[3]);
			long expectedCrc = Long.parseLong(parts[4], 16);

			System.out.println("Receiving chunk " + chunkId + " of " + fileName + " (size: " + chunkSize
					+ " bytes, expected CRC: " + Long.toHexString(expectedCrc) + ")");

			// Read exactly chunkSize bytes using DataInputStream for reliable reading
			byte[] chunkData = new byte[chunkSize];
			int totalRead = 0;
			while (totalRead < chunkSize) {
				int bytesRead = in.read(chunkData, totalRead, chunkSize - totalRead);
				if (bytesRead == -1) {
					throw new IOException(
							"Unexpected end of stream after reading " + totalRead + " of " + chunkSize + " bytes");
				}
				totalRead += bytesRead;
			}

			System.out.println("Read " + totalRead + " bytes for chunk " + chunkId);

			// Verify CRC32
			CRC32 crc = new CRC32();
			crc.update(chunkData);
			long actualCrc = crc.getValue();

			System.out.println("CRC Check - Expected: " + Long.toHexString(expectedCrc) + ", Actual: "
					+ Long.toHexString(actualCrc));

			if (actualCrc != expectedCrc) {
				System.err.println("Checksum mismatch for " + fileName + " chunk " + chunkId);
				System.err.println("   Expected: " + Long.toHexString(expectedCrc));
				System.err.println("   Actual:   " + Long.toHexString(actualCrc));
				// TODO: Send CHUNK_ERROR via UDP to owner
				return;
			}

			// Store chunk
			Path fileFolder = storageDir.resolve(fileName);
			Files.createDirectories(fileFolder);
			Path chunkFile = fileFolder.resolve("chunk" + chunkId);
			Files.write(chunkFile, chunkData);

			self.setNumberChunksStored(self.getNumberChunksStored() + 1);// Update number of chunks stored

			System.out.println("Stored chunk " + chunkId + " of file " + fileName + " at " + chunkFile);
			System.out.println("Chunk " + chunkId + " successfully received and verified");

			// Send acknowledgment back to owner via TCP (immediate feedback)
			String ack = "CHUNK_OK " + chunkId + "\n";
			out.write(ack.getBytes(StandardCharsets.UTF_8));
			out.flush();
			System.out.println("Sent TCP acknowledgment for chunk " + chunkId);

		} catch (Exception e) {
			System.err.println("Failed to handle TCP chunk from " + clientInfo + ": " + e.getMessage());
			e.printStackTrace();
		}
	}

	public static void restoreFileChunks(UDPClient client, String fileName, List<String> peers, int chunkSize, int totalChunks, String fileChecksum) {
		Path restored = Path.of("restored_" + fileName);

		try (FileOutputStream fos = new FileOutputStream(restored.toFile())) {

			System.out.println("Starting restoration for file " + fileName + "...");
			System.out.println("Restored file will be at: " + restored.toAbsolutePath());
			System.out.println("Total chunks to restore: " + totalChunks);

			// Store the checksum for later verification (if provided)
			if (fileChecksum != null && !fileChecksum.isEmpty()) {
				try {
					long checksumValue = Long.parseLong(fileChecksum, 16);
					fileChecksums.put(fileName, checksumValue);
					System.out.println("Stored expected file checksum: " + fileChecksum);
				} catch (NumberFormatException e) {
					System.err.println("Invalid file checksum format: " + fileChecksum);
				}
			}

			// Request each chunk in sequence
			for (int chunkId = 0; chunkId < totalChunks; chunkId++) {
				// Choose peer for this chunk (round-robin over the list)
				String[] parts = peers.get(chunkId % peers.size()).split(":");
				String peerName = parts[0];
				String peerIp = parts[1];
				int peerPort = Integer.parseInt(parts[2]);

				System.out.println("Requesting chunk " + chunkId + " from " + peerName + " at " + peerIp + ":" + peerPort);

				try (Socket socket = new Socket(peerIp, peerPort);
					OutputStream out = socket.getOutputStream();
					InputStream in = socket.getInputStream()) {

					// Send: GET_CHUNK RQ# File_Name Chunk_ID
					int rq = PeerMain.nextRequest();
					String header = String.format("GET_CHUNK %d %s %d\n", rq, fileName, chunkId);
					out.write(header.getBytes(StandardCharsets.UTF_8));
					out.flush();

					// Read response header line: CHUNK_DATA RQ# File_Name Chunk_ID Checksum
					StringBuilder hb = new StringBuilder();
					int c;
					while ((c = in.read()) != -1 && c != '\n') {
						hb.append((char) c);
					}
					String respHeader = hb.toString().trim();

					if (respHeader.isEmpty()) {
						System.out.println("Empty response for chunk " + chunkId + ". Assuming no more chunks.");
						sendRestoreFailedSafe(client, fileName, "Chunk_" + chunkId + "_Missing");
						return;
					}

					if (!respHeader.startsWith("CHUNK_DATA")) {
						System.out.println("Unexpected response for chunk " + chunkId + ": " + respHeader);
						sendRestoreFailedSafe(client, fileName, "Invalid_Response_For_Chunk_" + chunkId);
						return;
					}

					String[] respParts = respHeader.split("\\s+");
					if (respParts.length < 5) {
						System.err.println("Malformed CHUNK_DATA header: " + respHeader);
						sendRestoreFailedSafe(client, fileName, "Malformed_CHUNK_DATA_For_Chunk_" + chunkId);
						return;
					}

					String respFile = respParts[2];
					int respChunkId = Integer.parseInt(respParts[3]);
					String checksumHex = respParts[4];

					if (!respFile.equals(fileName) || respChunkId != chunkId) {
						System.err.println("Mismatched CHUNK_DATA header: " + respHeader);
						sendRestoreFailedSafe(client, fileName, "Mismatched_CHUNK_DATA");
						return;
					}

					// If checksum is the word ERROR, storage peer is telling us that the chunk does
					// not exist
					if ("ERROR".equalsIgnoreCase(checksumHex)) {
						System.out.println("Storage peer reports chunk " + chunkId + " not found. Stopping restore.");
						sendRestoreFailedSafe(client, fileName, "Chunk_" + chunkId + "_Not_Found");
                    	return;
					}

					long expectedCrc = Long.parseLong(checksumHex, 16);

					// Read chunk data
					byte[] buf = new byte[chunkSize];
					int bytesRead;
					int totalRead = 0;
					CRC32 crc = new CRC32();

					while ((bytesRead = in.read(buf)) != -1) {
						crc.update(buf, 0, bytesRead);
						fos.write(buf, 0, bytesRead);
						totalRead += bytesRead;
					}

					long actualCrc = crc.getValue();
					if (actualCrc != expectedCrc) {
						System.err.println("Checksum mismatch for chunk " + chunkId + " expected=" + checksumHex
								+ " actual=" + Long.toHexString(actualCrc));
						// We continue but final RESTORE will be marked as FAILED if file checksum does
						// not match
					} else {
						System.out.println("Chunk " + chunkId + " restored (" + totalRead + " bytes)");
					}

				} catch (IOException e) {
					System.err.println("Error requesting chunk " + chunkId + ": " + e.getMessage());
					// If we cannot get this chunk, we stop trying further ones
					sendRestoreFailedSafe(client, fileName, "Network_Error_Chunk_" + chunkId);
					return;
				}
			}

			System.out.println("File " + fileName + " restoration completed.");

			// Final file checksum verification
			Long expectedFileChecksum = fileChecksums.get(fileName);
			if (expectedFileChecksum == null) {
				System.out.println("No expected file checksum stored locally for " + fileName + ". Skipping final verification.");
				sendRestoreDoneSafe(client, fileName);
				return;
			}

			try (FileInputStream fis = new FileInputStream(restored.toFile())) {
				CRC32 finalCrc = new CRC32();
				byte[] buf = new byte[64 * 1024];
				int n;
				while ((n = fis.read(buf)) > 0) {
					finalCrc.update(buf, 0, n);
				}

				long finalChecksumValue = finalCrc.getValue();

				if (finalChecksumValue != expectedFileChecksum.longValue()) {
					System.err.println("FINAL CHECKSUM MISMATCH for restored file " + fileName + ": expected="
							+ Long.toHexString(expectedFileChecksum) + " actual=" + Long.toHexString(finalChecksumValue));
					sendRestoreFailedSafe(client, fileName, "Final_Checksum_Mismatch");
					return;
				}

				System.out.println("FINAL CHECKSUM VERIFIED for restored file " + fileName + ": " + Long.toHexString(finalChecksumValue));
				sendRestoreDoneSafe(client, fileName);


			} catch (IOException e) {
				System.err.println("Error writing restored file: " + e.getMessage());
				sendRestoreFailedSafe(client, fileName, "Final_Checksum_Computation_Error");
				e.printStackTrace();
			}

		} catch (IOException e) {
			System.err.println("Failed to recompute final checksum for restored file: " + e.getMessage());
			e.printStackTrace();
		}
	}

	public static synchronized int nextRequest() {
		return request++;
	}

	private static void sendRestoreDoneSafe(UDPClient client, String fileName) {
		try {
			client.sendRestoreDone(PeerMain.nextRequest(), fileName);
			System.out.println("Sent RESTORE_OK to server for file: " + fileName);
		} catch (TimeoutException e) {
			System.err.println("Timeout sending RESTORE_OK to server: " + e.getMessage());
		} catch (ExecutionException e) {
			System.err.println("Execution error sending RESTORE_OK to server: " + e.getMessage());
		} catch (InterruptedException e) {
			System.err.println("Interrupted while sending RESTORE_OK to server: " + e.getMessage());
			Thread.currentThread().interrupt();
		} catch (IOException e) {
			System.err.println("IO error sending RESTORE_OK to server: " + e.getMessage());
		}
	}

	private static void sendRestoreFailedSafe(UDPClient client, String fileName, String reason) {
		try {
			client.sendRestoreFailed(PeerMain.nextRequest(), fileName, reason);
			System.out.println("Sent RESTORE_FAIL to server for file: " + fileName + " reason: " + reason);
		} catch (TimeoutException e) {
			System.err.println("Timeout sending RESTORE_FAIL to server: " + e.getMessage());
		} catch (ExecutionException e) {
			System.err.println("Execution error sending RESTORE_FAIL to server: " + e.getMessage());
		} catch (InterruptedException e) {
			System.err.println("Interrupted while sending RESTORE_FAIL to server: " + e.getMessage());
			Thread.currentThread().interrupt();
		} catch (IOException e) {
			System.err.println("IO error sending RESTORE_FAIL to server: " + e.getMessage());
		}
	}

	private static void sendReplicateReqSafe(UDPClient client, String fileName, int chunkId, String targetPeer) {
		try {
			client.sendReplicateReq(PeerMain.nextRequest(), fileName, chunkId, targetPeer);
			System.out.println("Sent REPLICATE_REQ for " + fileName + " chunk " + chunkId + " to " + targetPeer);
		} catch (TimeoutException e) {
			System.err.println("Timeout sending REPLICATE_REQ: " + e.getMessage());
		} catch (ExecutionException e) {
			System.err.println("Execution error sending REPLICATE_REQ: " + e.getMessage());
		} catch (InterruptedException e) {
			System.err.println("Interrupted while sending REPLICATE_REQ: " + e.getMessage());
			Thread.currentThread().interrupt();
		} catch (IOException e) {
			System.err.println("IO error sending REPLICATE_REQ: " + e.getMessage());
		}
	}

	private static void processReplicateReq(UDPClient client, String message, Path storageDir) {
		// REPLICATE_REQ RQ# File_Name Chunk_ID Target_Peer
		String[] parts = message.split("\\s+");
		System.out.println("DEBUG REPLICATE_REQ: " + Arrays.toString(parts));

		if (parts.length < 5) {
			System.err.println("Malformed REPLICATE_REQ: " + message);
			return;
		}

		String rq = parts[1];
		String fileName = parts[2];
		int chunkId = Integer.parseInt(parts[3]);
		String targetPeer = parts[4];

		System.out.println("REPLICATE_REQ: Copying " + fileName + " chunk " + chunkId + " to " + targetPeer);

		// Debugging...show all known peers
		System.out.println("Known peers: ");
		for (Map.Entry<String, String> entry : storagePeerIps.entrySet()) {
			System.out.println("  " + entry.getKey() + " -> " + entry.getValue() + ":" + storagePeerPorts.get(entry.getKey()));
		}

		// Look up target peer connection info (we'll need to store this)
		String targetIp = storagePeerIps.get(targetPeer);
		Integer targetPort = storagePeerPorts.get(targetPeer);

		if (targetIp == null || targetPort == null) {
			System.err.println("ERROR: Unknown target peer for replication: " + targetPeer);
			System.err.println("Available peers: " + storagePeerIps.keySet());
			return;
		}

		System.out.println("Found target peer " + targetPeer + " is at " + targetIp + ":" + targetPort);

		// Perform the chunk replication
		boolean success = replicateChunkToPeer(fileName, chunkId, targetIp, targetPort, storageDir);
		
		if (success) {
			System.out.println("Successfully replicated " + fileName + " chunk " + chunkId + " to " + targetPeer);
			// Send REPLICATE_DONE confirmation to server
			try {
				client.sendReplicateDone(PeerMain.nextRequest(), fileName, chunkId, targetPeer);
				System.out.println("Sent REPLICATE_DONE to server for " + fileName + " chunk " + chunkId + " to " + targetPeer);
			} catch (Exception e) {
				System.err.println("Failed to send REPLICATE_DONE to server: " + e.getMessage());
			}
		} else {
			System.err.println("Failed to replicate " + fileName + " chunk " + chunkId + " to " + targetPeer);
		}
	}

	private static boolean replicateChunkToPeer(String fileName, int chunkId, String targetIp, int targetPort, Path storageDir) {
		try {
			Path chunkPath = storageDir.resolve(fileName).resolve("chunk" + chunkId);
			
			if (!Files.exists(chunkPath)) {
				System.err.println("Cannot replicate: Chunk not found: " + chunkPath);
				return false;
			}

			byte[] chunkData = Files.readAllBytes(chunkPath);
			
			// Calculate checksum
			CRC32 crc = new CRC32();
			crc.update(chunkData);
			String checksumHex = Long.toHexString(crc.getValue());

			System.out.println("Replicating chunk " + chunkId + " (" + chunkData.length + " bytes) to " + targetIp + ":" + targetPort);

			// Enhanced connection handling with timeout
			try (Socket socket = new Socket()) {
				socket.connect(new InetSocketAddress(targetIp, targetPort), 10000); // 10-second timeout
				socket.setSoTimeout(15000); // 15-second read timeout
				
				try (OutputStream out = socket.getOutputStream();
					InputStream in = socket.getInputStream();
					Scanner responseScanner = new Scanner(in, StandardCharsets.UTF_8.name())) {

					// Send replication header
					String header = "REPLICATE_CHUNK " + fileName + " " + chunkId + " " + 
								chunkData.length + " " + checksumHex + "\n";
					out.write(header.getBytes(StandardCharsets.UTF_8));
					out.write(chunkData);
					out.flush();

					System.out.println("Sent replication data to " + targetIp + ":" + targetPort);

					// Wait for acknowledgment with timeout
					if (responseScanner.hasNextLine()) {
						String ack = responseScanner.nextLine().trim();
						if (ack.startsWith("REPLICATE_OK")) {
							System.out.println("Replication acknowledged: " + ack);
							return true;
						} else {
							System.err.println("Unexpected replication response: " + ack);
						}
					} else {
						System.err.println("No acknowledgment received for replication");
					}
				}
			}
		} catch (SocketTimeoutException e) {
			System.err.println("Timeout during replication to " + targetIp + ":" + targetPort);
		} catch (IOException e) {
			System.err.println("IO error during replication: " + e.getMessage());
		} catch (Exception e) {
			System.err.println("Unexpected error during replication: " + e.getMessage());
			e.printStackTrace();
		}
		
		return false;
	}

	private static void handleReplicateChunk(String header, InputStream in, OutputStream out, Path storageDir, PeerNode self) {
    	// REPLICATE_CHUNK File_Name Chunk_ID Chunk_Size Checksum
		System.out.println("Handling REPLICATE_CHUNK...");

		String[] parts = header.split("\\s+");
		
		if (parts.length < 5) {
			System.err.println("Malformed REPLICATE_CHUNK header: " + header);
			return;
		}

		String fileName = parts[1];
		int chunkId = Integer.parseInt(parts[2]);
		int chunkSize = Integer.parseInt(parts[3]);
		long expectedCrc = Long.parseLong(parts[4], 16);

		System.out.println("Receiving replicated chunk " + chunkId + " of " + fileName + " (size: " + chunkSize + " bytes)");

		try {
			// Read chunk data
			byte[] chunkData = new byte[chunkSize];
			int totalRead = 0;
			while (totalRead < chunkSize) {
				int bytesRead = in.read(chunkData, totalRead, chunkSize - totalRead);
				if (bytesRead == -1) {
					throw new IOException("Unexpected end of stream during replication");
				}
				totalRead += bytesRead;
			}

			// Verify checksum
			CRC32 crc = new CRC32();
			crc.update(chunkData);
			long actualCrc = crc.getValue();

			if (actualCrc != expectedCrc) {
				System.err.println("Checksum mismatch for replicated chunk " + chunkId);
				return;
			}

			// Store the replicated chunk
			Path fileFolder = storageDir.resolve(fileName);
			Files.createDirectories(fileFolder);
			Path chunkFile = fileFolder.resolve("chunk" + chunkId);
			Files.write(chunkFile, chunkData);

			self.setNumberChunksStored(self.getNumberChunksStored() + 1);

			System.out.println("Stored replicated chunk " + chunkId + " of file " + fileName);
			
			// Send acknowledgment
			String ack = "REPLICATE_OK " + chunkId + "\n";
			out.write(ack.getBytes(StandardCharsets.UTF_8));
			out.flush();

		} catch (Exception e) {
			System.err.println("Failed to handle replicated chunk: " + e.getMessage());
			e.printStackTrace();
		}
	}
}
