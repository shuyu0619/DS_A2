package broker;

import remote.BrokerInterface;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.RemoteException;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * BrokerServer initializes the broker, binds it to the RMI registry,
 * and connects to other brokers specified via command-line arguments.
 */
public class BrokerServer {
    private static final Logger logger = Logger.getLogger(BrokerServer.class.getName());

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java -jar broker.jar <port> [-b broker_ip_1:port1 broker_ip_2:port2 ...]");
            System.exit(1);
        }

        int currentPort = Integer.parseInt(args[0]);

        List<String> otherBrokers = new ArrayList<>();

        // Parse optional -b flag and broker IP:port pairs
        for (int i = 1; i < args.length; i++) {
            if (args[i].equals("-b")) {
                i++;
                while (i < args.length) {
                    otherBrokers.add(args[i]);
                    i++;
                }
                break; // Remaining arguments are broker IP:port pairs
            } else {
                System.out.println("Invalid argument: " + args[i]);
                System.out.println("Usage: java -jar broker.jar <port> [-b broker_ip_1:port1 broker_ip_2:port2 ...]");
                System.exit(1);
            }
        }

        try {
            // Get the host address
            String hostname = InetAddress.getLocalHost().getHostAddress();

            // Create or get the RMI registry
            Registry registry;
            try {
                registry = LocateRegistry.createRegistry(currentPort);
                logger.info("RMI registry created on port " + currentPort);
            } catch (RemoteException e) {
                logger.info("RMI registry already exists on port " + currentPort + ", getting it.");
                registry = LocateRegistry.getRegistry(currentPort);
            }

            // Create a new BrokerImpl instance
            BrokerImpl broker = new BrokerImpl();
            broker.setBrokerIdentifier(hostname + ":" + currentPort);

            // Bind the broker service to the registry
            String serviceName = "BrokerService_" + currentPort;
            registry.rebind(serviceName, broker);
            logger.info("BrokerService bound with name: " + serviceName);
            System.out.println("BrokerServer started on " + hostname + ":" + currentPort);

            // Connect to other brokers specified via command-line arguments
            for (String brokerInfo : otherBrokers) {
                try {
                    String[] parts = brokerInfo.split(":");
                    String brokerIp = parts[0];
                    int brokerPort = Integer.parseInt(parts[1]);

                    Registry otherRegistry = LocateRegistry.getRegistry(brokerIp, brokerPort);
                    String otherServiceName = "BrokerService_" + brokerPort;
                    BrokerInterface otherBroker = (BrokerInterface) otherRegistry.lookup(otherServiceName);

                    String otherBrokerId = otherBroker.getBrokerIdentifier();
                    if (!broker.hasOtherBroker(otherBrokerId)) {
                        broker.addOtherBroker(otherBroker);
                        System.out.println("Connected to broker at " + brokerIp + ":" + brokerPort);
                        logger.info("Connected to broker at " + brokerIp + ":" + brokerPort);
                    }
                } catch (Exception e) {
                    System.err.println("Failed to connect to broker " + brokerInfo + ": " + e.getMessage());
                    logger.log(Level.SEVERE, "Failed to connect to broker " + brokerInfo + ": " + e.getMessage(), e);
                }
            }

            // Broker setup complete, now waiting for publishers and subscribers to connect

        } catch (UnknownHostException e) {
            logger.log(Level.SEVERE, "Failed to get host address: " + e.getMessage(), e);
        } catch (RemoteException e) {
            logger.log(Level.SEVERE, "Remote exception: " + e.getMessage(), e);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "BrokerServer exception: " + e.getMessage(), e);
        }
    }
}
