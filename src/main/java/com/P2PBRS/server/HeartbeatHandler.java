package com.P2PBRS.server;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.P2PBRS.peer.PeerNode;
import com.P2PBRS.server.ClientHandler.BackupManager;
import com.P2PBRS.server.RegistryManager.Result;

public class HeartbeatHandler extends Thread {
    private static final int HEARTBEAT_INTERVAL = 5000; // 5 seconds
    private static final int FAILURE_TIMEOUT = 15000;   // 15 seconds
    private final Map<String, Instant> lastHeartbeatTimes = new ConcurrentHashMap<>();
    private final Set<String> recoveringPeers = ConcurrentHashMap.newKeySet();

    // Testing timeout - 10 seconds
    private final long MAX_TIME = 10;
    
    private final RegistryManager registry = RegistryManager.getInstance();
    
    @Override
    public void run() {
        System.out.println("HeartbeatHandler started with " + MAX_TIME + " second timeout");

        while (true) {            
            List<PeerNode> list = registry.listPeers();
            
            for (int i = 0; i < list.size(); i++) {
                PeerNode p = list.get(i);    
                
                Instant now = Instant.now();
                Instant lastTimestamp = p.getLastTimestamp();
                if (lastTimestamp == null) {
                    p.setLastTimestamp(now);
                    lastTimestamp = p.getLastTimestamp();
                }
                
                Duration diff = Duration.between(lastTimestamp, now);
                long timeSinceLastTimestamp = diff.getSeconds();
                
                if (timeSinceLastTimestamp > MAX_TIME) {
                    if (!recoveringPeers.contains(p.getName())) {
                        System.out.println("Peer " + p.getName() + " marked as down. Time since last heartbeat: " + timeSinceLastTimestamp + " seconds");
                    
                        // FIRST: Mark as recovering and trigger recovery for this failed peer's chunks
                        recoveringPeers.add(p.getName());
                        triggerRecoveryForFailedPeer(p.getName());

                        System.out.println("Down client " + p.getName() + " DEREGISTERING");

                        // THEN: Deregister the failed peer
                        Result result = registry.deregisterPeer(p.getName());
                        if (!result.ok) {
                            System.err.println("Failed to deregister client " + p.getName());
                        }
                    }
                }
            }
            
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void triggerRecoveryForFailedPeer(String failedPeerName) {
        System.out.println("Triggering recovery for failed peer: " + failedPeerName);

        // Remove the failed peer's chunk locations
        registry.removeChunkLocationsForPeer(failedPeerName);
        
        // Get all backup plans
        Map<String, BackupManager.Plan> plans = BackupManager.getInstance().getPlans();
        
        int totalChunksToRecover = 0;
        int successfulRecoveries = 0;

        for (BackupManager.Plan plan : plans.values()) {
            // Find chunks stored by the failed peer
            Map<Integer, String> chunksToRecover = new HashMap<>();
            
            for (Map.Entry<Integer, PeerNode> entry : plan.placement.entrySet()) {
                if (entry.getValue().getName().equals(failedPeerName)) {
                    chunksToRecover.put(entry.getKey(), entry.getValue().getName());
                }
            }
            
            if (!chunksToRecover.isEmpty()) {
                System.out.println("Found " + chunksToRecover.size() + " chunks to recover from file: " + plan.fileName);
                totalChunksToRecover += chunksToRecover.size();
                
                // For each chunk, find a new storage peer and trigger replication
                for (Map.Entry<Integer, String> chunkEntry : chunksToRecover.entrySet()) {
                    int chunkId = chunkEntry.getKey();
                    String newStoragePeer = selectNewStoragePeer(plan.owner, plan.fileName, failedPeerName);
                    
                    if (newStoragePeer != null) {
                        Boolean success = triggerChunkReplication(plan.fileName, chunkId, newStoragePeer);

                        if (success) {
                            successfulRecoveries++;
                        }
                    } else {
                        System.err.println("No available peer to store chunk " + chunkId + " of " + plan.fileName);
                    }
                }
            }
        }

        System.out.println("Recovery summary: " + successfulRecoveries + "/" + totalChunksToRecover + " chunks scheduled for recovery");
        
        // Remove from recovering set after a delay (to prevent repeated recovery attempts)
        new Thread(() -> {
            try {
                Thread.sleep(30000); // Wait 30 seconds before allowing recovery again
                recoveringPeers.remove(failedPeerName);
                System.out.println("Removed " + failedPeerName + " from recovering peers set");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).start();
    }

    private String selectNewStoragePeer(String ownerName, String fileName, String failedPeer) {
        List<PeerNode> allPeers = registry.listPeers();
        List<PeerNode> candidates = new ArrayList<>();
        
        // Find available storage peers (excluding the owner and failed peer)
        for (PeerNode peer : allPeers) {
            if (!peer.getName().equals(ownerName) && 
                !peer.getName().equals(failedPeer) &&
                ("STORAGE".equals(peer.getRole()) || "BOTH".equals(peer.getRole())) &&
                peer.getStorageCapacity() > 0) {
                candidates.add(peer);
            }
        }
        
        if (candidates.isEmpty()) {
            System.err.println("No available storage peers for recovery!");
            return null;
        }
        
        // Select peer with most available capacity
        candidates.sort((a, b) -> Integer.compare(b.getStorageCapacity(), a.getStorageCapacity()));
        String selectedPeer = candidates.get(0).getName();
        
        System.out.println("Selected new storage peer: " + selectedPeer + " for recovery");
        return selectedPeer;
    }

    private Boolean triggerChunkReplication(String fileName, int chunkId, String targetPeer) {
        System.out.println("Attempting recovery: " + fileName + " chunk " + chunkId + " -> " + targetPeer);
        
        // Find which peer currently has this chunk
        String sourcePeer = findPeerWithChunk(fileName, chunkId);
        
        if (sourcePeer != null && !sourcePeer.equals(targetPeer)) {
            // Send REPLICATE_REQ to the source peer
            sendReplicateRequest(sourcePeer, fileName, chunkId, targetPeer);
            System.out.println("Recovery scheduled: " + fileName + " chunk " + chunkId + " from " + sourcePeer + " to " + targetPeer);
            return true;
        } else if (sourcePeer == null) {
            System.err.println("Recovery impossible: No available source for " + fileName + " chunk " + chunkId);
            System.err.println("   Chunk is permanently lost (only copy was on failed peer)");
        } else {
            System.err.println("Recovery failed: Source and target are same peer: " + sourcePeer);
        }
        return false;
    }

    private String findPeerWithChunk(String fileName, int chunkId) {    
        System.out.println("Searching for " + fileName + " chunk " + chunkId);

        // FIRST: Try to find the chunk using RegistryManager's chunk tracking
        String chunkPeer = registry.findPeerWithChunk(fileName, chunkId);
        if (chunkPeer != null) {
            System.out.println("Found chunk via registry: " + chunkPeer);
            
            // Check if this peer is actually responsive
            Optional<PeerNode> peerNode = registry.getPeer(chunkPeer);
            if (peerNode.isPresent() && isPeerActuallyAlive(peerNode.get())) {
                return chunkPeer;
            }
        }
        
        // FALLBACK: Use backup plans
        Map<String, BackupManager.Plan> plans = BackupManager.getInstance().getPlans();
        
        for (BackupManager.Plan plan : plans.values()) {
            if (plan.fileName.equals(fileName)) {
                PeerNode storagePeer = plan.placement.get(chunkId);
                
                if (storagePeer != null) {
                    System.out.println("Found storage peer in plan: " + storagePeer.getName());
                    
                    // Check if this peer is actually responsive
                    boolean isActuallyAlive = isPeerActuallyAlive(storagePeer);
                    System.out.println("Peer " + storagePeer.getName() + " is actually alive: " + isActuallyAlive);
                    
                    if (isActuallyAlive) {
                        return storagePeer.getName();
                    } else {
                        // Find alternative storage peer (NOT file owner)
                        System.out.println("Storage peer " + storagePeer.getName() + " is unresponsive, finding alternative...");
                        return findAlternativeStoragePeer(fileName, chunkId, storagePeer.getName());
                    }
                }
            }
        }
        
        System.out.println("Could not find source peer for " + fileName + " chunk " + chunkId);
        return null;
    }

    private String findAlternativeStoragePeer(String fileName, int chunkId, String excludePeer) {
        System.out.println("Looking for alternative storage peer for " + fileName + " chunk " + chunkId);
        
        // First, try to find which peers actually have this chunk (using registry)
        Set<String> allPeersWithThisChunk = findAllPeersWithChunk(fileName, chunkId, excludePeer);
        
        for (String peerName : allPeersWithThisChunk) {
            Optional<PeerNode> peer = registry.getPeer(peerName);
            if (peer.isPresent() && isPeerActuallyAlive(peer.get())) {
                System.out.println("Found peer with chunk: " + peerName);
                return peerName;
            }
        }
        
        // If no peer has the chunk, find any available storage peer that's NOT the file owner
        List<PeerNode> allPeers = registry.listPeers();
        
        // First, find out who owns this file
        String fileOwner = findFileOwner(fileName);
        System.out.println("File owner: " + fileOwner);
        
        for (PeerNode peer : allPeers) {
            // Skip: failed peer, file owner, and non-storage peers
            if (!peer.getName().equals(excludePeer) && 
                !peer.getName().equals(fileOwner) &&  // CRITICAL: Don't use file owner!
                ("STORAGE".equals(peer.getRole()) || "BOTH".equals(peer.getRole())) &&
                isPeerActuallyAlive(peer)) {
                
                System.out.println("Trying alternative storage peer: " + peer.getName());
                return peer.getName();
            }
        }
        
        System.out.println("No alternative storage peers found");
        return null;
    }

    private Set<String> findAllPeersWithChunk(String fileName, int chunkId, String excludePeer) {
        Set<String> peersWithChunk = new HashSet<>();
        
        // Check all backup plans to see which peers have this chunk
        Map<String, BackupManager.Plan> plans = BackupManager.getInstance().getPlans();
        for (BackupManager.Plan plan : plans.values()) {
            if (plan.fileName.equals(fileName)) {
                for (Map.Entry<Integer, PeerNode> entry : plan.placement.entrySet()) {
                    if (entry.getKey() == chunkId && !entry.getValue().getName().equals(excludePeer)) {
                        peersWithChunk.add(entry.getValue().getName());
                    }
                }
            }
        }
        
        System.out.println("Peers with chunk " + chunkId + ": " + peersWithChunk);
        return peersWithChunk;
    }

    private String findFileOwner(String fileName) {
        Map<String, BackupManager.Plan> plans = BackupManager.getInstance().getPlans();
        for (BackupManager.Plan plan : plans.values()) {
            if (plan.fileName.equals(fileName)) {
                return plan.owner;
            }
        }
        return null;
    }

    private boolean isPeerActuallyAlive(PeerNode peer) {
        try {
            // Try to establish a TCP connection to the peer's storage port
            java.net.Socket socket = new java.net.Socket();
            socket.connect(new java.net.InetSocketAddress(peer.getIpAddress(), peer.getTcpPort()), 2000); // 2 second timeout
            socket.close();
            return true;
        } catch (Exception e) {
            System.out.println("Peer " + peer.getName() + " is not reachable at " + peer.getIpAddress() + ":" + peer.getTcpPort());
            return false;
        }
    }

    private void sendReplicateRequest(String sourcePeer, String fileName, int chunkId, String targetPeer) {
        try {
            Optional<PeerNode> sourcePeerNode = registry.getPeer(sourcePeer);
            if (sourcePeerNode.isPresent()) {
                PeerNode peer = sourcePeerNode.get();
                
                // Create a UDP client to send the replication request
                java.net.DatagramSocket socket = new java.net.DatagramSocket();
                String message = String.format("REPLICATE_REQ %d %s %d %s", 
                    generateRequestId(), fileName, chunkId, targetPeer);
                
                byte[] data = message.getBytes();
                java.net.DatagramPacket packet = new java.net.DatagramPacket(
                    data, data.length, 
                    java.net.InetAddress.getByName(peer.getIpAddress()), 
                    peer.getUdpPort()
                );
                
                socket.send(packet);
                socket.close();
                
                System.out.println("Sent REPLICATE_REQ to " + sourcePeer + " for " + fileName + " chunk " + chunkId);
            } else {
                System.err.println("Source peer not found: " + sourcePeer);
            }
        } catch (Exception e) {
            System.err.println("Failed to send REPLICATE_REQ to " + sourcePeer + ": " + e.getMessage());
        }
    }

    private int generateRequestId() {
        return (int) (System.currentTimeMillis() % 1000000);
    }
}