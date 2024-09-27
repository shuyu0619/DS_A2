package publisher;

import remote.BrokerInterface;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.RemoteException;
import java.util.Scanner;
import java.util.List;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * PublisherClient allows publishers to interact with the broker.
 */
public class PublisherClient {
    private static final Logger logger = Logger.getLogger(PublisherClient.class.getName());

    private final String publisherName;
    private BrokerInterface broker;
    private final Scanner scanner;

    public PublisherClient(String publisherName, String brokerIp, int brokerPort) {
        this.publisherName = publisherName;
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
        while (true) {
            displayMenu();
            String command = scanner.nextLine().trim();

            try {
                switch (command.toLowerCase()) {
                    case "create":
                        createTopic();
                        break;
                    case "publish":
                        publishMessage();
                        break;
                    case "show":
                        showSubscriberCount();
                        break;
                    case "delete":
                        deleteTopic();
                        break;
                    case "exit":
                        System.out.println("Exiting publisher.");
                        logger.info("Publisher " + publisherName + " is exiting.");
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
        System.out.println("\nPlease select command: create, publish, show, delete.");
        System.out.print("Enter command: ");
    }

    private void createTopic() throws RemoteException {
        System.out.print("Enter topic ID: ");
        String topicId = scanner.nextLine().trim();
        System.out.print("Enter topic name: ");
        String topicName = scanner.nextLine().trim();

        if (topicId.isEmpty() || topicName.isEmpty()) {
            throw new IllegalArgumentException("Topic ID and name cannot be empty.");
        }

        broker.createTopic(topicId, topicName, publisherName);
        System.out.println("success");
        logger.info("Created topic " + topicId + " - " + topicName);
    }

    private void publishMessage() throws RemoteException {
        System.out.print("Enter topic ID: ");
        String topicId = scanner.nextLine().trim();
        System.out.print("Enter message: ");
        String message = scanner.nextLine().trim();

        if (message.isEmpty()) {
            throw new IllegalArgumentException("Message cannot be empty.");
        }
        if (message.length() > 100) {
            throw new IllegalArgumentException("Message exceeds 100 characters.");
        }

        broker.publishMessage(topicId, message, publisherName);
        System.out.println("success");
        logger.info("Published message to topic " + topicId + ": " + message);
    }

    private void showSubscriberCount() throws RemoteException {
        System.out.print("Enter topic ID: ");
        String topicId = scanner.nextLine().trim();

        if (topicId.isEmpty()) {
            throw new IllegalArgumentException("Topic ID cannot be empty.");
        }

        List<String> topicInfo = broker.getSubscriberCount(topicId, publisherName);
        if (topicInfo.isEmpty()) {
            System.out.println("[No subscribers]");
        } else {
            for (String info : topicInfo) {
                System.out.println("[" + info + "]");
            }
        }
        logger.info("Displayed subscriber count for topic " + topicId);
    }

    private void deleteTopic() throws RemoteException {
        System.out.print("Enter topic ID: ");
        String topicId = scanner.nextLine().trim();

        if (topicId.isEmpty()) {
            throw new IllegalArgumentException("Topic ID cannot be empty.");
        }

        broker.deleteTopic(topicId, publisherName);
        System.out.println("success");
        logger.info("Deleted topic " + topicId);
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: java -jar publisher.jar <username> <broker_ip> <broker_port>");
            System.exit(1);
        }

        String publisherName = args[0];
        String brokerIp = args[1];
        int brokerPort;
        try {
            brokerPort = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            System.out.println("error: Broker port must be a valid integer.");
            return;
        }

        PublisherClient client = new PublisherClient(publisherName, brokerIp, brokerPort);
        client.start();
    }
}
