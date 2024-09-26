package subscriber;

import remote.BrokerInterface;
import remote.SubscriberCallbackInterface;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;
import java.util.Scanner;

public class SubscriberClient {
    private String subscriberName;
    private BrokerInterface broker;
    private Scanner scanner;

    public SubscriberClient(String subscriberName) {
        this.subscriberName = subscriberName;
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
        SubscriberCallbackInterface callback = null;
        try {
            callback = new SubscriberImpl(subscriberName);
        } catch (RemoteException e) {
            System.err.println("Failed to create subscriber callback: " + e.getMessage());
            System.exit(1);
        }

        while (true) {
            System.out.println("Please select command: list, sub, current, unsub.");
            String command = scanner.nextLine().trim();

            try {
                switch (command) {
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
                        System.out.println("Exiting...");
                        return;
                    default:
                        System.out.println("error: Invalid command.");
                }
            } catch (RemoteException e) {
                System.out.println("error: Remote operation failed: " + e.getMessage());
            } catch (Exception e) {
                System.out.println("error: An unexpected error occurred: " + e.getMessage());
            }
        }
    }

    private void listTopics() throws RemoteException {
        List<String> topics = broker.listTopics();
        if (topics.isEmpty()) {
            System.out.println("No topics available.");
        } else {
            for (String topic : topics) {
                System.out.println(topic);
            }
        }
    }

    private void subscribeTopic(SubscriberCallbackInterface callback) throws RemoteException {
        System.out.print("Enter topic ID: ");
        String topicId = scanner.nextLine().trim();
        broker.subscribe(topicId, subscriberName, callback);
        System.out.println("success!");
    }

    private void showCurrentSubscriptions() throws RemoteException {
        List<String> subscriptions = broker.getCurrentSubscriptions(subscriberName);
        if (subscriptions.isEmpty()) {
            System.out.println("No current subscriptions.");
        } else {
            for (String subscription : subscriptions) {
                System.out.println(subscription);
            }
        }
    }

    private void unsubscribeTopic() throws RemoteException {
        System.out.print("Enter topic ID: ");
        String topicId = scanner.nextLine().trim();
        broker.unsubscribe(topicId, subscriberName);
        System.out.println("success!");
    }

    public static void main(String[] args) {
        System.out.print("Enter your name: ");
        String subscriberName = new Scanner(System.in).nextLine();
        new SubscriberClient(subscriberName).start();
    }
}