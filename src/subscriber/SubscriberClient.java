package subscriber;

import remote.BrokerInterface;
import remote.SubscriberCallbackInterface;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;
import java.util.Scanner;

public class SubscriberClient {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        try {
            // get subscriber name
            System.out.print("Enter your name: ");
            String subscriberName = scanner.nextLine();

            // get RMI registry
            Registry registry = LocateRegistry.getRegistry("localhost", 1099);

            // find remote object
            BrokerInterface broker = (BrokerInterface) registry.lookup("BrokerService");

            // create subscriber callback object
            SubscriberCallbackInterface callback = new SubscriberImpl(subscriberName);

            // main loop
            while (true) {
                System.out.println("Commands: list, subscribe {topicId}, unsubscribe {topicId}, exit");
                System.out.print("Enter command: ");
                String input = scanner.nextLine();

                try {
                    if (input.equals("exit")) {
                        break;
                    } else if (input.equals("list")) {
                        List<String> topics = broker.listTopics();
                        if (topics.isEmpty()) {
                            System.out.println("No topics available.");
                        } else {
                            System.out.println("Available topics:");
                            for (String topic : topics) {
                                System.out.println("- " + topic);
                            }
                        }
                    } else if (input.startsWith("subscribe ")) {
                        String topicId = input.substring(10).trim();
                        broker.subscribe(topicId, subscriberName, callback);
                        System.out.println("Subscribed to topic: " + topicId);
                    } else if (input.startsWith("unsubscribe ")) {
                        String topicId = input.substring(12).trim();
                        broker.unsubscribe(topicId, subscriberName);
                        System.out.println("Unsubscribed from topic: " + topicId);
                    } else {
                        System.out.println("Invalid command.");
                    }
                } catch (Exception e) {
                    System.err.println("Error: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            System.err.println("Initialization error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            scanner.close();
            System.out.println("Subscriber exited.");
        }
    }
}
