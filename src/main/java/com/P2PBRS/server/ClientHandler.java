package com.p2pbrs.server;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

public class ClientHandler extends Thread {
    private DatagramPacket packet;
    private DatagramSocket socket;

    public ClientHandler(DatagramPacket packet, DatagramSocket socket) {
        this.packet = packet;
        this.socket = socket;
    }

    @Override
    public void run() {
        try {
            // Extract message from packet
            String receivedData = new String(packet.getData(), 0, packet.getLength());
            System.out.println(Thread.currentThread().getName() + " | Received packet from " + packet.getAddress() + ":" + packet.getPort() + " --> Data: " + receivedData);

            // Process the message based on type
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
        // TODO: Parse REGISTER message components:
        // Format: REGISTER RQ# Name Role IP_Address UDP_Port# TCP_Port# Storage_Capacity
        // Example: REGISTER 01 Alice BOTH 192.168.1.10 5001 6001 1024MB
        
        // TODO: Extract components using MessageParser
        // TODO: Validate role is one of: OWNER, STORAGE, BOTH
        // TODO: Validate storage capacity format (e.g., 1024MB)
        // TODO: Check if name is already registered using RegistryManager
        // TODO: Check if server can handle more clients
        // TODO: Register peer in RegistryManager

        return "REGISTERED Successfully";
    }

    private String processDeregisterMessage(String message) {
        // TODO: Parse DE-REGISTER message components:
        // Format: DE-REGISTER RQ# Name
        // Example: DE-REGISTER 01 Alice
        
        // TODO: Extract components using MessageParser
        // TODO: Remove peer from RegistryManager

        return "DE-REGISTERED Successfully";
    }
}



