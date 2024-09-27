package subscriber;

import remote.BrokerInterface;
import remote.SubscriberCallbackInterface;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.RemoteException;
import java.util.Scanner;
import java.util.List;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * SubscriberClient allows subscribers to interact with the broker.
 */
public class SubscriberClient {
    private static final Logger logger = Logger.getLogger(SubscriberClient.class.getName());

    private final String subscriberName;
    private BrokerInterface broker;
    private final Scanner scanner;

    public SubscriberClient(String subscriberName, String brokerIp, int brokerPort) {
        this.subscriberName = subscriberName;
        this.scanner = new Scanner(System.in);
        try {
            Registry registry = LocateRegistry.getRegistry(brokerIp, brokerPort);

            // Service name is "BrokerService_<port>"
            String serviceName = "BrokerService_" + brokerPort;
            broker = (BrokerInterface) registry.lookup(serviceName);
            logger.info("Connected to BrokerService at " + brokerIp + ":" + brokerPort);
            System.out.println("Connected to BrokerService at " + brokerIp + ":" + brokerPort);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Unable to connect to BrokerService: {0}", e.getMessage());
            System.err.println("Unable to connect to BrokerService: " + e.getMessage());
            System.exit(1);
        }
    }

    public void start() {
        SubscriberCallbackInterface callback;
        try {
            callback = new SubscriberImpl(subscriberName);
            logger.info("Subscriber callback created for " + subscriberName);
        } catch (RemoteException e) {
            System.err.println("Failed to create subscriber callback: " + e.getMessage());
            logger.log(Level.SEVERE, "Failed to create subscriber callback: {0}", e.getMessage());
            System.exit(1);
            return;
        }

        while (true) {
            displayMenu();
            String command = scanner.nextLine().trim();

            try {
                switch (command.toLowerCase()) {
                    case "list":
                        listTopics();
                        break;
                    case "sub":
                        subscribeTopic(callback);
                        break;
                    case "current":
                        showCurrentSubscriptions();
                        break;
                    case "unsub":
                        unsubscribeTopic();
                        break;
                    case "exit":
                        System.out.println("Exiting subscriber.");
                        logger.info("Subscriber " + subscriberName + " is exiting.");
                        return;
                    default:
                        System.out.println("error: Invalid command.");
                        logger.warning("Invalid command entered: " + command);
                }
            } catch (RemoteException e) {
                System.out.println("error: Remote operation failed: " + e.getMessage());
                logger.log(Level.SEVERE, "Remote operation failed: {0}", e.getMessage());
            } catch (IllegalArgumentException e) {
                System.out.println("error: " + e.getMessage());
                logger.warning("Invalid input: " + e.getMessage());
            } catch (Exception e) {
                System.out.println("error: An unexpected error occurred: " + e.getMessage());
                logger.log(Level.SEVERE, "Unexpected error: {0}", e.getMessage());
            }
        }
    }

    private void displayMenu() {
        System.out.println("\nPlease select command: list, sub, current, unsub.");
        System.out.print("Enter command: ");
    }

    private void listTopics() throws RemoteException {
        List<String> topics = broker.listTopics();
        if (topics.isEmpty()) {
            System.out.println("[No topics available]");
        } else {
            for (String topic : topics) {
                System.out.println("[" + topic + "]");
            }
        }
        logger.info("Listed all topics.");
    }

    private void subscribeTopic(SubscriberCallbackInterface callback) throws RemoteException {
        System.out.print("Enter topic ID: ");
        String topicId = scanner.nextLine().trim();
        if (topicId.isEmpty()) {
            throw new IllegalArgumentException("Topic ID cannot be empty.");
        }
        broker.subscribe(topicId, subscriberName, callback);
        System.out.println("success");
        logger.info("Subscribed to topic " + topicId);
    }

    private void showCurrentSubscriptions() throws RemoteException {
        List<String> subscriptions = broker.getCurrentSubscriptions(subscriberName);
        if (subscriptions.isEmpty()) {
            System.out.println("[No current subscriptions]");
        } else {
            for (String subscription : subscriptions) {
                System.out.println("[" + subscription + "]");
            }
        }
        logger.info("Displayed current subscriptions for " + subscriberName);
    }

    private void unsubscribeTopic() throws RemoteException {
        System.out.print("Enter topic ID: ");
        String topicId = scanner.nextLine().trim();
        if (topicId.isEmpty()) {
            throw new IllegalArgumentException("Topic ID cannot be empty.");
        }
        broker.unsubscribe(topicId, subscriberName);
        System.out.println("success");
        logger.info("Unsubscribed from topic " + topicId);
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: java -jar subscriber.jar <username> <broker_ip> <broker_port>");
            System.exit(1);
        }
        String subscriberName = args[0];
        String brokerIp = args[1];
        int brokerPort;
        try {
            brokerPort = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            System.out.println("error: Broker port must be a valid integer.");
            return;
        }

        SubscriberClient client = new SubscriberClient(subscriberName, brokerIp, brokerPort);
        client.start();
    }
}
