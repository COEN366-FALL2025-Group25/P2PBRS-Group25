package com.P2PBRS.server;

import com.P2PBRS.peer.PeerNode;

import java.net.DatagramPacket;
import java.net.DatagramSocket;

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
            System.out.println(Thread.currentThread().getName() + " | Received packet from " +
                    packet.getAddress() + ":" + packet.getPort() + " --> Data: " + receivedData);

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
        }
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
        // Format: REGISTER RQ# Name Role IP_Address UDP_Port# TCP_Port# Storage_Capacity
        String[] components = message.split("\\s+");
        if (components.length < 8) {
            return "ERROR: Malformed REGISTER message";
        }

        String rqNumber = components[1];
        String name = components[2];
        String role = components[3];
        String ipAddress = components[4];

        int udpPort;
        int tcpPort;
        int storageCapacity;

        try {
            udpPort = Integer.parseInt(components[5]);
            tcpPort = Integer.parseInt(components[6]);
            storageCapacity = Integer.parseInt(components[7]);
        } catch (NumberFormatException e) {
            return "ERROR: Invalid numeric field in REGISTER";
        }

        PeerNode peer = new PeerNode(name, role, ipAddress, udpPort, tcpPort, storageCapacity);
        RegistryManager.Result result = registry.registerPeer(peer);

        if (!result.ok) {
            return result.message + " " + rqNumber + " " + name;
        }
        return "REGISTERED " + rqNumber + " " + name;
    }

    private String processDeregisterMessage(String message) {
        // Format: DE-REGISTER RQ# Name
        String[] components = message.split("\\s+");
        if (components.length < 3) {
            return "ERROR: Malformed DE-REGISTER message";
        }
        String rqNumber = components[1];
        String name = components[2];

        RegistryManager.Result result = registry.deregisterPeer(name);
        if (!result.ok) {
            return result.message + " " + rqNumber;
        }
        return "DE-REGISTERED " + rqNumber;
    }
}
