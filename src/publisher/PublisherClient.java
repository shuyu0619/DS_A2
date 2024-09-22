package publisher;

import remote.BrokerInterface;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;
import java.util.Scanner;

public class PublisherClient {
    private String publisherName;
    private BrokerInterface broker;
    private Scanner scanner;

    public PublisherClient(String publisherName) {
        this.publisherName = publisherName;
        this.scanner = new Scanner(System.in);
        try {
            //get RMI registry
            Registry registry = LocateRegistry.getRegistry("localhost", 1099);

            //find remote object
            broker = (BrokerInterface) registry.lookup("BrokerService");
        } catch (Exception e) {
            System.err.println("Unable to connect to BrokerService: " + e.getMessage());
            System.exit(1);
        }
    }

    public void start() {
        System.out.println("Welcome " + publisherName);

        while (true) {
            System.out.println("\nAvailable commands:");
            System.out.println("1. create - Create a new topic");
            System.out.println("2. publish - Publish a message to a topic");
            System.out.println("3. list - List all topics");
            System.out.println("4. exit - Exit the program");
            System.out.print("Enter command: ");

            String command = scanner.nextLine().trim();

            switch (command.toLowerCase()) {
                case "create":
                    createTopic();
                    break;
                case "publish":
                    publishMessage();
                    break;
                case "list":
                    listTopics();
                    break;
                case "exit":
                    System.out.println("Bye!");
                    scanner.close();
                    return;
                default:
                    System.out.println("Invalid command, please try again.");
            }
        }
    }

    private void createTopic() {
        try {
            System.out.print("Enter topic ID: ");
            String topicId = scanner.nextLine();
            System.out.print("Enter topic name: ");
            String topicName = scanner.nextLine();
            broker.createTopic(topicId, topicName, publisherName);
            System.out.println("Topic created: " + topicId + " - " + topicName);
        } catch (Exception e) {
            System.err.println("Failed to create topic: " + e.getMessage());
        }
    }

    private void publishMessage() {
        try {
            System.out.print("Enter topic ID: ");
            String topicId = scanner.nextLine();
            System.out.println("Enter message content (type 'exit' to return to main menu):");
            while (true) {
                String message = scanner.nextLine();
                if ("exit".equalsIgnoreCase(message.trim())) {
                    System.out.println("Stopped publishing messages, returning to main menu.");
                    break;
                }
                broker.publishMessage(topicId, message, publisherName);
                System.out.println("Message published: " + message);
                System.out.println("Continue entering message content (or type 'exit' to return):");
            }
        } catch (Exception e) {
            System.err.println("Failed to publish message: " + e.getMessage());
        }
    }

    private void listTopics() {
        try {
            List<String> topics = broker.listTopics();
            if (topics.isEmpty()) {
                System.out.println("No topics available at the moment.");
            } else {
                System.out.println("List of available topics:");
                for (String topic : topics) {
                    System.out.println("- " + topic);
                }
            }
        } catch (Exception e) {
            System.err.println("Failed to retrieve topic list: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        // Get publisher name
        Scanner scanner = new Scanner(System.in);
        System.out.print("Please enter your name: ");
        String publisherName = scanner.nextLine();
        PublisherClient client = new PublisherClient(publisherName);
        client.start();
    }
}
