package com.P2PBRS.peer;

public class PeerNode {
    private final String name;
    private final String role; // OWNER | STORAGE | BOTH
    private final String ipAddress;
    private final int udpPort;
    private final int tcpPort;
    private final int storageCapacity;

    public PeerNode(String name, String role, String ipAddress,
                    int udpPort, int tcpPort, int storageCapacity) {
        this.name = name;
        this.role = role;
        this.ipAddress = ipAddress;
        this.udpPort = udpPort;
        this.tcpPort = tcpPort;
        this.storageCapacity = storageCapacity;
    }

    // Getters
    public String getName() { return name; }
    public String getRole() { return role; }
    public String getIpAddress() { return ipAddress; }
    public int getUdpPort() { return udpPort; }
    public int getTcpPort() { return tcpPort; }
    public int getStorageCapacity() { return storageCapacity; }
}
