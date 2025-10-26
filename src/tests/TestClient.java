import java.net.*;

public class TestClient {
    public static void main(String[] args) throws Exception {
        DatagramSocket clientSocket = new DatagramSocket();
        InetAddress serverAddress = InetAddress.getByName("localhost");
        
        String message1 = "REGISTER 01 Alice BOTH 192.168.1.10 5001 6001 1024MB";
        byte[] sendData = message1.getBytes();
        
        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, serverAddress, 5000);
        clientSocket.send(sendPacket);
        System.out.println("Sent: " + message1);

        String message2 = "DE-REGISTER 01";
        sendData = message2.getBytes();
        sendPacket = new DatagramPacket(sendData, sendData.length, serverAddress, 5000);
        clientSocket.send(sendPacket);
        System.out.println("Sent: " + message2);
        
        clientSocket.close();
    }
}