package com.P2PBRS.server;

import com.P2PBRS.peer.PeerNode;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class ClientHandler extends Thread {
	private final DatagramPacket packet;
	private final DatagramSocket socket;
	private final RegistryManager registry = RegistryManager.getInstance();

	public ClientHandler(DatagramPacket packet, DatagramSocket socket) {
		this.packet = packet;
		this.socket = socket;
	}

	@Override
	public void run() {
		try {
			String receivedData = new String(packet.getData(), 0, packet.getLength());
			System.out.println(Thread.currentThread().getName() + " | Received from " + packet.getAddress() + ":"
					+ packet.getPort() + " --> " + receivedData);

			String responseData = processMessage(receivedData);
			sendResponse(responseData);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private String processMessage(String message) {
		if (message.startsWith("REGISTER")) {
			return processRegisterMessage(message);
		} else if (message.startsWith("DE-REGISTER")) {
			return processDeregisterMessage(message);
		} else if (message.startsWith("BACKUP_REQ")) {
			return processBackupReq(message);
		} else if (message.startsWith("BACKUP_DONE")) {
			return processBackupDone(message);
		} else if (message.startsWith("HEARTBEAT")) {
			return processHeartbeat(message);
		} else if (message.startsWith("RESTORE_REQ")) {
			return processRestoreReq(message);
		} else if (message.startsWith("RESTORE_OK")) {
			// RESTORE_OK RQ# File_Name
			String[] c = message.split("\\s+");
			if (c.length < 3) {
				return "ERROR: Malformed RESTORE_OK";
			}
			String rq = c[1];
			String fileName = c[2];
			return "RESTORE_CONFIRMED " + rq + " " + fileName;
		} else if (message.startsWith("RESTORE_FAIL")) {
			// RESTORE_FAIL RQ# File_Name Reason
			String[] c = message.split("\\s+");
			if (c.length < 4) {
				return "ERROR: Malformed RESTORE_FAIL";
			}
			String rq = c[1];
			String fileName = c[2];
			String reason = c[3];
			return "RESTORE_FAILED " + rq + " " + fileName + " " + reason;
		} else if (message.startsWith("REPLICATE_REQ")) {
			return processReplicateReq(message);
		} else if (message.startsWith("REPLICATE_DONE")) {
    		return processReplicateDone(message);
		}
		return "ERROR: Unknown command";
	}

	private void sendResponse(String response) {
		try {
			byte[] responseData = response.getBytes();
			DatagramPacket responsePacket = new DatagramPacket(responseData, responseData.length, packet.getAddress(),
					packet.getPort());
			socket.send(responsePacket);
			System.out.println(Thread.currentThread().getName() + " | Sent response: " + response);
		} catch (Exception e) {
			System.err.println("Failed to send response: " + e.getMessage());
		}
	}

	private String processRegisterMessage(String message) {
		// REGISTER RQ# Name Role IP_Address UDP_Port# TCP_Port# Storage_Capacity
		String[] c = message.split("\\s+");
		if (c.length < 8) {
			return "ERROR: Malformed REGISTER message";
		}
		String rqNumber = c[1];
		String name = c[2];
		String role = c[3];
		String ipAddress = c[4];

		int udpPort, tcpPort, storageCapacity;
		try {
			udpPort = Integer.parseInt(c[5]);
			tcpPort = Integer.parseInt(c[6]);
			storageCapacity = Integer.parseInt(c[7]);
		} catch (NumberFormatException e) {
			return "ERROR: Invalid numeric field in REGISTER";
		}

		PeerNode peer = new PeerNode(name, role, ipAddress, udpPort, tcpPort, storageCapacity);
		RegistryManager.Result result = registry.registerPeer(peer);
		if (!result.ok)
			return result.message + " " + rqNumber + " " + name;

		sendPeerListToPeer(peer);

		return "REGISTERED " + rqNumber + " " + name;
	}

	private String processDeregisterMessage(String message) {
		// DE-REGISTER RQ# Name
		String[] c = message.split("\\s+");
		if (c.length < 3)
			return "ERROR: Malformed DE-REGISTER message";
		String rqNumber = c[1];
		String name = c[2];

		RegistryManager.Result result = registry.deregisterPeer(name);
		if (!result.ok)
			return result.message + " " + rqNumber;
		return "DE-REGISTERED " + rqNumber;
	}

	private String processBackupReq(String message) {
		// Format: BACKUP_REQ RQ# File_Name File_Size Checksum [Chunk_Size]
		String[] c = message.split("\\s+");
		if (c.length < 5)
			return "ERROR: Malformed BACKUP_REQ";
		String rq = c[1];
		String fileName = c[2];

		long fileSize;
		try {
			fileSize = Long.parseLong(c[3]);
		} catch (NumberFormatException e) {
			return "ERROR: Invalid File_Size";
		}

		String checksum = c[4];
		int requestedChunkSize = (c.length >= 6) ? Integer.parseInt(c[5]) : 4096;
		int chunkSize = Math.max(1024, Math.min(requestedChunkSize, 1 << 20)); // clamp 1KB..1MB

		// Identify owner by source endpoint (requires client to bind its UDP socket to
		// its registered udpPort)
		Optional<PeerNode> maybeOwner = findPeerByEndpoint(packet.getAddress(), packet.getPort());
		if (maybeOwner.isEmpty()) {
			return "BACKUP-DENIED " + rq + " Owner_Not_Registered";
		}
		PeerNode owner = maybeOwner.get();

		// Select candidate storage peers (with debugging)
		List<PeerNode> allPeers = registrySnapshot();
		System.out.println("=== DEBUG: All registered peers ===");
		for (PeerNode p : allPeers) {
			System.out.println("  - " + p.getName() + " (role: " + p.getRole() + ") at " + p.getIpAddress() + ":"
					+ p.getUdpPort() + " UDP, " + p.getTcpPort() + " TCP, capacity: " + p.getStorageCapacity());
		}

		List<PeerNode> candidates = allPeers.stream().filter(p -> !p.getName().equals(owner.getName()))
				.filter(p -> "STORAGE".equals(p.getRole()) || "BOTH".equals(p.getRole()))
				.filter(p -> p.getStorageCapacity() > 0) // Only peers with available capacity
				.collect(Collectors.toList());

		System.out.println("=== DEBUG: Available storage peers ===");
		for (PeerNode p : candidates) {
			System.out.println("  - " + p.getName() + " capacity: " + p.getStorageCapacity());
		}

		if (candidates.isEmpty())
			return "BACKUP-DENIED " + rq + " No_Available_Storage";

		int numChunks = (int) ((fileSize + chunkSize - 1) / chunkSize);
		if (candidates.size() < numChunks)
			return "BACKUP-DENIED " + rq + " Not_Enough_Peers";

		int fanout = Math.min(numChunks, candidates.size());
		// Sort by available capacity (descending) and take the top ones
		List<PeerNode> selected = candidates.stream()
				.sorted((a, b) -> Integer.compare(b.getStorageCapacity(), a.getStorageCapacity())).limit(fanout)
				.collect(Collectors.toList());

		System.out.println("=== DEBUG: Selected storage peers ===");
		for (PeerNode p : selected) {
			System.out.println("  - SELECTED: " + p.getName() + " for backup");
		}

		// Round-robin placement: chunkId -> storage peer
		Map<Integer, PeerNode> placement = new java.util.HashMap<>();
		for (int i = 0; i < numChunks; i++)
			placement.put(i, selected.get(i % selected.size()));

		// Save plan
		BackupManager.Plan plan = new BackupManager.Plan(owner.getName(), fileName, checksum, chunkSize, fileSize, placement);
		BackupManager.getInstance().putPlan(plan);

		// Notify each selected storage peer with ONLY their assigned chunks
		for (PeerNode sp : selected) {
			String task = String.format("STORAGE_TASK %s %s %d %s", rq, fileName, chunkSize, owner.getName());
			sendUdp(task, sp.getIpAddress(), sp.getUdpPort());

			// Find chunks assigned to THIS specific storage peer
			List<Integer> chunksForThisPeer = placement.entrySet().stream()
					.filter(e -> e.getValue().getName().equals(sp.getName())).map(Map.Entry::getKey)
					.collect(Collectors.toList());

			System.out.println("Assigning chunks " + chunksForThisPeer + " to " + sp.getName());

			// Send STORE_REQ for each chunk assigned to this peer
			for (int chunkId : chunksForThisPeer) {
				String storeReq = String.format("STORE_REQ %s %s %d %s", rq, fileName, chunkId, owner.getName());
				sendUdp(storeReq, sp.getIpAddress(), sp.getUdpPort());
			}
		}

		String peerList = selected.stream()
				.map(p -> String.format("%s:%s:%d", p.getName(), p.getIpAddress(), p.getTcpPort()))
				.collect(Collectors.joining(","));

		System.out.println("=== DEBUG: Final peer connection details ===");
		for (PeerNode sp : selected) {
			registry.debugPrintPeer(sp.getName());
		}

		return String.format("BACKUP_PLAN %s %s [%s] %d", rq, fileName, peerList, chunkSize);
	}

	private String processBackupDone(String message) {
		// BACKUP_DONE RQ# File_Name
		String[] c = message.split("\\s+");
		if (c.length < 3)
			return "ERROR: Malformed BACKUP_DONE";
		String rq = c[1];
		String fileName = c[2];

		Optional<PeerNode> maybeOwner = findPeerByEndpoint(packet.getAddress(), packet.getPort());
		if (maybeOwner.isEmpty()) {
			return "ERROR: Unknown owner for BACKUP_DONE";
		}
		String ownerName = maybeOwner.get().getName();

		BackupManager.Plan plan = BackupManager.getInstance().getPlan(ownerName, fileName);
		if (plan == null) {
			return "ERROR: No plan found for BACKUP_DONE";
		}

		BackupManager.getInstance().markDone(ownerName, fileName);
		return "BACKUP_DONE " + rq + " " + fileName;
	}

	private List<PeerNode> registrySnapshot() {
		return registry.listPeers();
	}

	private String processHeartbeat(String message) {
		String[] c = message.split("\\s+");
		String rq = c[1];
		String name = c[2];
		int numberChunks;
		try {
			numberChunks = Integer.parseInt(c[3]);
		} catch (NumberFormatException e) {
			return "ERROR: Invalid numeric field in HEARTBEAT";
		}

		String timestamp = c[4];

		// Check if the name is inside the list of names
		Optional<PeerNode> maybePeer = registry.getPeer(name);
		if (maybePeer.isEmpty()) {
			return "HEARTBEAT " + rq + " ERROR Client not found";
		} else {
			// Update the number of chunks and timestamps of that node
			PeerNode peer = maybePeer.get();
			peer.setNumberChunksStored(numberChunks);
			peer.setLastHeartbeatTime(timestamp);
			peer.setLastTimestamp(Instant.now());

			return "HEARTBEAT " + rq + " of node " + name + " OK";
		}
	}

	private String processRestoreReq(String message) {
		// RESTORE_REQ RQ# File_Name
		String[] c = message.split("\\s+");
		if (c.length < 3)
			return "ERROR: Malformed RESTORE_REQ";

		String rq = c[1];
		String fileName = c[2];

		// Identify the peer sending the request (the owner)
		Optional<PeerNode> maybeOwner = findPeerByEndpoint(packet.getAddress(), packet.getPort());
		if (maybeOwner.isEmpty()) {
			return "RESTORE-DENIED " + rq + " Owner_Not_Registered";
		}
		String ownerName = maybeOwner.get().getName();

		// Recover previous backup plan
		BackupManager.Plan plan = BackupManager.getInstance().getPlan(ownerName, fileName);
		if (plan == null) {
			return "RESTORE-DENIED " + rq + " No_Backup_Found";
		}

		BackupManager.getInstance().markDone(ownerName, fileName);

		// Build list of peers storing chunks
		Map<Integer, PeerNode> placement = plan.placement;
		if (placement == null || placement.isEmpty()) {
			return "RESTORE-DENIED " + rq + " No_Storage_Peers";
		}

		// Debug
		System.out.println("=== DEBUG: Restore request for " + fileName + " ===");
		for (Map.Entry<Integer, PeerNode> e : placement.entrySet()) {
			System.out.println(" - Chunk " + e.getKey() + " stored at " + e.getValue().getName());
		}

		// Build restore plan string with total chunks and checksum
		List<String> peersList = new ArrayList<>();
		for (PeerNode p : new HashSet<>(placement.values())) { // uniq peers
			peersList.add(String.format("%s:%s:%d", p.getName(), p.getIpAddress(), p.getTcpPort()));
		}

		String peerString = String.join(",", peersList);
		return String.format("RESTORE_PLAN %s %s [%s] %d %d %s", rq, fileName, peerString, plan.chunkSize, plan.totalChunks, plan.checksumHex);	
	}

	private Optional<PeerNode> findPeerByEndpoint(InetAddress addr, int udpPort) {
		return registry.findPeerByEndpoint(addr, udpPort);
	}

	private void sendUdp(String msg, String ip, int port) {
		try {
			byte[] data = msg.getBytes();
			DatagramPacket dp = new DatagramPacket(data, data.length, InetAddress.getByName(ip), port);
			socket.send(dp);
			System.out.println("Notify " + ip + ":" + port + " <= " + msg);
		} catch (Exception e) {
			System.err.println("Failed to notify " + ip + ":" + port + " : " + e.getMessage());
		}
	}

	static class BackupManager {
		private static final BackupManager INSTANCE = new BackupManager();

		static BackupManager getInstance() {
			return INSTANCE;
		}

		static class Plan {
			final String owner;
			final String fileName;
			final String checksumHex;
			final int chunkSize;
			final long fileSize;
			final int totalChunks;
			final Map<Integer, PeerNode> placement; // chunkId -> storage peer
			volatile boolean done;

			Plan(String owner, String fileName, String checksumHex, int chunkSize, long fileSize, Map<Integer, PeerNode> placement) {
				this.owner = owner;
				this.fileName = fileName;
				this.checksumHex = checksumHex;
				this.chunkSize = chunkSize;
				this.fileSize = fileSize;
				this.totalChunks = (int) ((fileSize + chunkSize - 1) / chunkSize);
				this.placement = new ConcurrentHashMap<>(placement);
				this.done = false;
			}
		}

		private final Map<String, Plan> plans = new ConcurrentHashMap<>();

		public Map<String, Plan> getPlans() {
			return new ConcurrentHashMap<>(plans); // Return a copy for thread safety
		}

		private String key(String owner, String file) {
			return owner + "||" + file;
		}

		void putPlan(Plan p) {
			plans.put(key(p.owner, p.fileName), p);
		}

		Plan getPlan(String owner, String file) {
			return plans.get(key(owner, file));
		}

		void markDone(String owner, String file) {
			Plan p = getPlan(owner, file);
			if (p != null)
				p.done = true;
		}
	}

	private String processReplicateReq(String message) {
		// REPLICATE_REQ RQ# File_Name Chunk_ID Target_Peer
		String[] c = message.split("\\s+");
		if (c.length < 5) {
			return "ERROR: Malformed REPLICATE_REQ";
		}

		String rq = c[1];
		String fileName = c[2];
		int chunkId;
		try {
			chunkId = Integer.parseInt(c[3]);
		} catch (NumberFormatException e) {
			return "ERROR: Invalid Chunk_ID";
		}
		String targetPeer = c[4];

		System.out.println("Processing REPLICATE_REQ for file " + fileName + " chunk " + chunkId + " to " + targetPeer);

		// Find which backup plan contains this file
		BackupManager.Plan foundPlan = null;
		for (BackupManager.Plan plan : BackupManager.getInstance().getPlans().values()) {
			if (plan.fileName.equals(fileName) && plan.placement.containsKey(chunkId)) {
				foundPlan = plan;
				break;
			}
		}

		if (foundPlan == null) {
			return "REPLICATE_FAIL " + rq + " Backup plan not found for file: " + fileName;
		}

		// Find the source peer that currently stores this chunk
		PeerNode sourcePeer = foundPlan.placement.get(chunkId);
		if (sourcePeer == null) {
			return "REPLICATE_FAIL " + rq + " Chunk " + chunkId + " not found in backup plan";
		}

		// Find the target peer
		Optional<PeerNode> targetPeerNode = registry.getPeer(targetPeer);
		if (targetPeerNode.isEmpty()) {
			return "REPLICATE_FAIL " + rq + " Target peer not found: " + targetPeer;
		}

		System.out.println("Found source peer: " + sourcePeer.getName() + " has chunk " + chunkId);
		System.out.println("Target peer: " + targetPeerNode.get().getName());

		// Send REPLICATE_REQ to the source peer (the one that has the chunk)
		String replicateMsg = String.format("REPLICATE_REQ %s %s %d %s", rq, fileName, chunkId, targetPeer);
		sendUdp(replicateMsg, sourcePeer.getIpAddress(), sourcePeer.getUdpPort());

		System.out.println("Forwarded REPLICATE_REQ to " + sourcePeer.getName() + " to copy chunk to " + targetPeer);

		return "REPLICATE_ACK " + rq + " " + fileName + " " + chunkId + " " + targetPeer;
	}

	private Optional<PeerNode> findPeerByName(String peerName) {
		return registry.getPeer(peerName);
	}

	private void sendPeerListToPeer(PeerNode targetPeer) {
		List<PeerNode> allPeers = registry.listPeers();
		for (PeerNode peer : allPeers) {
			if (!peer.getName().equals(targetPeer.getName())) {
				// Send peer info to the new peer
				String peerInfo = String.format("PEER_INFO %s %s %d", 
					peer.getName(), peer.getIpAddress(), peer.getTcpPort());
				sendUdp(peerInfo, targetPeer.getIpAddress(), targetPeer.getUdpPort());
			}
		}
	}

	private String processReplicateDone(String message) {
		// REPLICATE_DONE RQ# File_Name Chunk_ID Target_Peer
		String[] c = message.split("\\s+");
		if (c.length < 5) {
			return "ERROR: Malformed REPLICATE_DONE";
		}
		
		String rq = c[1];
		String fileName = c[2];
		int chunkId = Integer.parseInt(c[3]);
		String targetPeer = c[4];
		
		System.out.println("Replication completed: " + fileName + " chunk " + chunkId + " to " + targetPeer);
		return "REPLICATE_DONE " + rq + " OK";
	}
}
