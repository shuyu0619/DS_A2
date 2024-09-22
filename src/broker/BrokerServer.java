package broker;

import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;

public class BrokerServer {
    public static void main(String[] args) {
        try {
            // start RMI registry
            try {
                LocateRegistry.createRegistry(1099);
                System.out.println("RMI registry started on port 1099.");
            } catch (Exception e) {
                System.out.println("RMI registry already running.");
            }

            // create Broker
            BrokerInterface broker = new BrokerImpl();

            // bind Broker
            Naming.rebind("rmi://localhost:1099/BrokerService", broker);
            System.out.println("BrokerService is ready.");
        } catch (Exception e) {
            System.err.println("BrokerServer exception: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
