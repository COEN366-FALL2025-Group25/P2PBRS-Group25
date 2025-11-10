package com.P2PBRS.server;

import com.P2PBRS.peer.PeerNode;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
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
            System.out.println(Thread.currentThread().getName() + " | Received from " +
                    packet.getAddress() + ":" + packet.getPort() + " --> " + receivedData);

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
        }
        // (You can add CHUNK_OK, CHUNK_ERROR, STORE_ACK handlers here later.)
        return "ERROR: Unknown command";
    }

    private void sendResponse(String response) {
        try {
            byte[] responseData = response.getBytes();
            DatagramPacket responsePacket = new DatagramPacket(
                responseData,
                responseData.length,
                packet.getAddress(),
                packet.getPort()
            );
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
        if (!result.ok) return result.message + " " + rqNumber + " " + name;
        return "REGISTERED " + rqNumber + " " + name;
    }

    private String processDeregisterMessage(String message) {
        // DE-REGISTER RQ# Name
        String[] c = message.split("\\s+");
        if (c.length < 3) return "ERROR: Malformed DE-REGISTER message";
        String rqNumber = c[1];
        String name = c[2];

        RegistryManager.Result result = registry.deregisterPeer(name);
        if (!result.ok) return result.message + " " + rqNumber;
        return "DE-REGISTERED " + rqNumber;
    }

    private String processBackupReq(String message) {
        // Format: BACKUP_REQ RQ# File_Name File_Size Checksum [Chunk_Size]
        String[] c = message.split("\\s+");
        if (c.length < 5) return "ERROR: Malformed BACKUP_REQ";
        String rq = c[1];
        String fileName = c[2];

        // Parse file size
        long fileSize;
        try { fileSize = Long.parseLong(c[3]); }
        catch (NumberFormatException e) { return "ERROR: Invalid File_Size"; }

        // Parse checksum
        String checksum = c[4];
        int requestedChunkSize = (c.length >= 6) ? Integer.parseInt(c[5]) : 4096;
        int chunkSize = Math.max(1024, Math.min(requestedChunkSize, 1 << 20)); // clamp 1KB..1MB

        // Identify owner by source endpoint (requires client to bind its UDP socket to its registered udpPort)
        System.out.println("DEBUG: Looking for owner at " + packet.getAddress() + ":" + packet.getPort());

        Optional<PeerNode> maybeOwner = findPeerByEndpoint(packet.getAddress(), packet.getPort());

        System.out.println("DEBUG: Found owner: " + maybeOwner.isPresent());
        if (maybeOwner.isPresent()) {
            System.out.println("DEBUG: Owner details: " + maybeOwner.get().getName() + " UDP: " + maybeOwner.get().getUdpPort());
        }
        if (maybeOwner.isEmpty()) {
            return "BACKUP-DENIED " + rq + " Owner_Not_Registered";
        }
        PeerNode owner = maybeOwner.get();

        // Select candidate storage peers (naive)
        List<PeerNode> candidates = registrySnapshot().stream()
                .filter(p -> !p.getName().equals(owner.getName()))
                .filter(p -> "STORAGE".equals(p.getRole()) || "BOTH".equals(p.getRole()))
                .toList();
        if (candidates.isEmpty()) return "BACKUP-DENIED " + rq + " No_Available_Storage";

        int numChunks = (int) ((fileSize + chunkSize - 1) / chunkSize);
        if (candidates.size() < numChunks) return "BACKUP-DENIED " + rq + " Not_Enough_Peers";

        int fanout = Math.min(numChunks, candidates.size());
        List<PeerNode> selected = candidates.subList(0, fanout);

        // Round-robin placement: chunkId -> storage peer
        Map<Integer, PeerNode> placement = new java.util.HashMap<>();
        for (int i = 0; i < numChunks; i++) placement.put(i, selected.get(i % selected.size()));

        // Save plan
        BackupManager.Plan plan = new BackupManager.Plan(owner.getName(), fileName, checksum, chunkSize, fileSize, placement);
        BackupManager.getInstance().putPlan(plan);

        // Notify each selected storage peer
        for (PeerNode sp : selected) {
            String task = String.format("STORAGE_TASK %s %s %d %s", rq, fileName, chunkSize, owner.getName());
            sendUdp(task, sp.getIpAddress(), sp.getUdpPort());

            for (var e : placement.entrySet()) {
                if (e.getValue().getName().equals(sp.getName())) {
                    String storeReq = String.format("STORE_REQ %s %s %d %s", rq, fileName, e.getKey(), owner.getName());
                    sendUdp(storeReq, sp.getIpAddress(), sp.getUdpPort());
                }
            }
        }

        String peerList = selected.stream().map(PeerNode::getName).collect(java.util.stream.Collectors.joining(","));
        return String.format("BACKUP_PLAN %s %s [%s] %d", rq, fileName, peerList, chunkSize);


    }

    private String processBackupDone(String message) {
        // BACKUP_DONE RQ# File_Name
        String[] c = message.split("\\s+");
        if (c.length < 3) return "ERROR: Malformed BACKUP_DONE";
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
        static BackupManager getInstance() { return INSTANCE; }

        static class Plan {
            final String owner;
            final String fileName;
            final String checksumHex;
            final int chunkSize;
            final long fileSize;
            final Map<Integer, PeerNode> placement; // chunkId -> storage peer
            volatile boolean done;

            Plan(String owner, String fileName, String checksumHex, int chunkSize, long fileSize, Map<Integer, PeerNode> placement) {
                this.owner = owner;
                this.fileName = fileName;
                this.checksumHex = checksumHex;
                this.chunkSize = chunkSize;
                this.fileSize = fileSize;
                this.placement = new ConcurrentHashMap<>(placement);
                this.done = false;
            }
        }

        private final Map<String, Plan> plans = new ConcurrentHashMap<>();
        private String key(String owner, String file) { return owner + "||" + file; }

        void putPlan(Plan p) { plans.put(key(p.owner, p.fileName), p); }
        Plan getPlan(String owner, String file) { return plans.get(key(owner, file)); }
        void markDone(String owner, String file) {
            Plan p = getPlan(owner, file);
            if (p != null) p.done = true;
        }
    }
}
