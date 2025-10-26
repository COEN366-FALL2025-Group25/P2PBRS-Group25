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
        // Format: REGISTER RQ# Name Role IP_Address UDP_Port# TCP_Port# Storage_Capacity
        String [] components = message.split(" ");
        String RQNumber = components[1];
        String name = components[2];
        String role = components[3];
        String ipAddress = components[4];
        String udpPort = components[5];
        String tcpPort = components[6];
        String storageCapacity = components[7];
        
        // for testing...
        // if (role.equals("OWNER")) {
        //     System.out.println("Role is OWNER");
        // } else if (role.equals("STORAGE")) {
        //     System.out.println("Role is STORAGE");
        // } else if (role.equals("BOTH")) {
        //     System.out.println("Role is BOTH");
        // } else {
        //     return "ERROR: Invalid role specified";
        // }

        // System.out.println("Storage Capacity: " + storageCapacity);

        // TODO: Check if name is already registered using RegistryManager
        // TODO: check if IP and ports are already in use
        // TODO: check if server capacity exceeded
        // return errors if so

        // TODO: Register peer in RegistryManager

        return "REGISTERED " + RQNumber + " " + name;
    }

    private String processDeregisterMessage(String message) {
        // Format: DE-REGISTER RQ# Name
        
        String [] components = message.split(" ");
        String RQNumber = components[1];
        // TODO: Remove peer from RegistryManager

        return "DE-REGISTERED " + RQNumber;
    }
}



