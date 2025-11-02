package com.P2PBRS.peer;

import java.time.Instant;
import java.util.Objects;

public class PeerNode {
    private String name;
    private String role;           // OWNER | STORAGE | BOTH
    private String ipAddress;
    private int udpPort;
    private int tcpPort;
    private int storageCapacity;
    private Instant registeredAt;

    public PeerNode() { }

    public PeerNode(String name, String role, String ipAddress,
                    int udpPort, int tcpPort, int storageCapacity) {
        this.name = name;
        this.role = role;
        this.ipAddress = ipAddress;
        this.udpPort = udpPort;
        this.tcpPort = tcpPort;
        this.storageCapacity = storageCapacity;
        this.registeredAt = Instant.now();
    }

    public String getName() { return name; }
    public String getRole() { return role; }
    public String getIpAddress() { return ipAddress; }
    public int getUdpPort() { return udpPort; }
    public int getTcpPort() { return tcpPort; }
    public int getStorageCapacity() { return storageCapacity; }
    public String getRegisteredAt() { return registeredAt == null ? null : registeredAt.toString(); }

    public void setName(String name) { this.name = name; }
    public void setRole(String role) { this.role = role; }
    public void setIpAddress(String ipAddress) { this.ipAddress = ipAddress; }
    public void setUdpPort(int udpPort) { this.udpPort = udpPort; }
    public void setTcpPort(int tcpPort) { this.tcpPort = tcpPort; }
    public void setStorageCapacity(int storageCapacity) { this.storageCapacity = storageCapacity; }

    public void setRegisteredAt(String registeredAt) {
        this.registeredAt = (registeredAt == null || registeredAt.isBlank())
                ? null
                : Instant.parse(registeredAt);
    }
    public void setRegisteredAt(Instant registeredAt) { this.registeredAt = registeredAt; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PeerNode)) return false;
        PeerNode that = (PeerNode) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
