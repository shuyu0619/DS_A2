package publisher;

import remote.BrokerInterface;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Scanner;
import java.util.List;

public class PublisherClient {
    private String publisherName;
    private BrokerInterface broker;
    private Scanner scanner;

    public PublisherClient(String publisherName) {
        this.publisherName = publisherName;
        this.scanner = new Scanner(System.in);
        try {
            Registry registry = LocateRegistry.getRegistry("localhost", 1099);
            broker = (BrokerInterface) registry.lookup("BrokerService");
        } catch (Exception e) {
            System.err.println("Unable to connect to BrokerService: " + e.getMessage());
            System.exit(1);
        }
    }

    public void start() {
        while (true) {
            System.out.println("Please select command: create, publish, show, delete.");
            String command = scanner.nextLine().trim();

            try {
                switch (command) {
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
                        return;
                    default:
                        System.out.println("error Invalid command.");
                }
            } catch (Exception e) {
                System.out.println("error " + e.getMessage());
            }
        }
    }

    private void createTopic() throws Exception {
        System.out.print("Enter topic ID and name: ");
        String[] inputs = scanner.nextLine().split(" ", 2);
        if (inputs.length != 2) {
            throw new IllegalArgumentException("Invalid input. Please provide topic ID and name.");
        }
        broker.createTopic(inputs[0], inputs[1], publisherName);
        System.out.println("created!");
    }

    private void publishMessage() throws Exception {
        System.out.print("Enter topic ID and message: ");
        String[] inputs = scanner.nextLine().split(" ", 2);
        if (inputs.length != 2) {
            throw new IllegalArgumentException("Invalid input. Please provide topic ID and message.");
        }
        broker.publishMessage(inputs[0], inputs[1], publisherName);
        System.out.println("published!");
    }

    private void showSubscriberCount() throws Exception {
        System.out.print("Enter topic ID: ");
        String topicId = scanner.nextLine().trim();
        List<String> topicInfo = broker.getSubscriberCount(topicId, publisherName);
        for (String info : topicInfo) {
            System.out.println(info);
        }
    }

    private void deleteTopic() throws Exception {
        System.out.print("Enter topic ID: ");
        String topicId = scanner.nextLine().trim();
        broker.deleteTopic(topicId, publisherName);
        System.out.println("deleted!");
    }

    public static void main(String[] args) {
        System.out.print("Enter your name: ");
        String publisherName = new Scanner(System.in).nextLine();
        new PublisherClient(publisherName).start();
    }
}