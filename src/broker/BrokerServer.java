package broker;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.net.InetAddress;

public class BrokerServer {
    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: java -jar broker.jar <port> <directory_ip> <directory_port>");
            System.exit(1);
        }

        int currentPort = Integer.parseInt(args[0]);
        String directoryIp = args[1];
        int directoryPort = Integer.parseInt(args[2]);

        try {
            String hostname = InetAddress.getLocalHost().getHostAddress();
            Registry registry = LocateRegistry.createRegistry(currentPort);


            BrokerImpl broker = new BrokerImpl(currentPort, directoryIp, directoryPort);


            String serviceName = "BrokerService_" + currentPort;
            registry.rebind(serviceName, broker);
            System.out.println("BrokerServer started on " + hostname + ":" + currentPort);


            broker.initializeConnections();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
