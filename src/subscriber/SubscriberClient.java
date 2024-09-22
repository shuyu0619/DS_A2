package subscriber;

import broker.BrokerInterface;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Scanner;

public class SubscriberClient {
    public static void main(String[] args) {
        try {
            // get subscriber name
            Scanner scanner = new Scanner(System.in);
            System.out.print("Enter your name: ");
            String subscriberName = scanner.nextLine();

            // get broker service
            BrokerInterface broker = (BrokerInterface) Naming.lookup("rmi://localhost:1099/BrokerService");

            // create subscriber
            SubscriberCallbackInterface callback = new SubscriberImpl(subscriberName);

            // subscribe to topics
            while (true) {
                System.out.println("Commands: list, subscribe {topicId}, exit");
                System.out.print("Enter command: ");
                String input = scanner.nextLine();

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
                } else {
                    System.out.println("Invalid command.");
                }
            }
            scanner.close();
            System.out.println("Subscriber exited.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
