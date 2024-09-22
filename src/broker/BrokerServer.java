package broker;

import remote.BrokerInterface;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class BrokerServer {
    public static void main(String[] args) {
        try {
            // create on port 1099
            Registry registry = LocateRegistry.createRegistry(1099);
            System.out.println("RMI registry started on port 1099.");

            // create a new service
            BrokerInterface broker = new BrokerImpl();

            // register the service
            registry.rebind("BrokerService", broker);
            System.out.println("BrokerService is ready.");
        } catch (Exception e) {
            System.err.println("BrokerServer exception: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
