package subscriber;

import remote.BrokerInterface;
import remote.DirectoryServiceInterface;
import remote.BrokerInfo;
import remote.SubscriberCallbackInterface;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Scanner;
import java.util.List;
import java.util.Random;


public class SubscriberClient {
    private final String subscriberName;
    private BrokerInterface broker;
    private final Scanner scanner;
    private volatile boolean isRunning = true;

    public SubscriberClient(String subscriberName, String directoryIp, int directoryPort) {
        this.subscriberName = subscriberName;
        this.scanner = new Scanner(System.in);
        connectToBroker(directoryIp, directoryPort);
    }

    private void connectToBroker(String directoryIp, int directoryPort) {
        try {
            Registry directoryRegistry = LocateRegistry.getRegistry(directoryIp, directoryPort);
            DirectoryServiceInterface directoryService = (DirectoryServiceInterface) directoryRegistry.lookup("DirectoryService");

            List<BrokerInfo> brokers = directoryService.getBrokerList();
            if (brokers.isEmpty()) {
                System.err.println("No brokers available.");
                System.exit(1);
            }

            Random rand = new Random();
            BrokerInfo brokerInfo = brokers.get(rand.nextInt(brokers.size()));

            Registry registry = LocateRegistry.getRegistry(brokerInfo.getIp(), brokerInfo.getPort());
            String serviceName = "BrokerService_" + brokerInfo.getPort();
            broker = (BrokerInterface) registry.lookup(serviceName);

            System.out.println("Connected to BrokerService at " + brokerInfo.getIp() + ":" + brokerInfo.getPort());

            // 启动心跳线程
            startHeartbeat();

        } catch (Exception e) {
            System.err.println("Unable to connect to BrokerService: " + e.getMessage());
            System.exit(1);
        }
    }

    public void start() {
        SubscriberCallbackInterface callback;
        try {
            callback = new SubscriberImpl();
        } catch (Exception e) {
            System.err.println("Failed to create subscriber callback: " + e.getMessage());
            System.exit(1);
            return;
        }

        while (isRunning) {
            System.out.println("Please select command: list, sub, current, unsub");
            String commandLine = scanner.nextLine().trim();
            String[] parts = commandLine.split("\\s+", 2);
            if (parts.length == 0) {
                System.out.println("error: Invalid command.");
                continue;
            }
            String command = parts[0];
            try {
                switch (command.toLowerCase()) {
                    case "list":
                        listTopics();
                        break;
                    case "sub":
                        if (parts.length != 2) {
                            System.out.println("error: Invalid command format.");
                            break;
                        }
                        subscribeTopic(parts[1], callback);
                        break;
                    case "current":
                        showCurrentSubscriptions();
                        break;
                    case "unsub":
                        if (parts.length != 2) {
                            System.out.println("error: Invalid command format.");
                            break;
                        }
                        unsubscribeTopic(parts[1]);
                        break;
                    default:
                        System.out.println("error: Invalid command.");
                }
            } catch (Exception e) {
                System.out.println("error: " + e.getMessage());
            }
        }
        System.out.println("Subscriber client exited.");
    }

    private void listTopics() throws Exception {
        List<String> topics = broker.listTopics();
        if (topics.isEmpty()) {
            System.out.println("No topics available");
        } else {
            for (String topic : topics) {
                String[] parts = topic.split("\\s+");
                System.out.println("[" + parts[0] + "] [" + parts[1] + "] [" + parts[2] + "]");
            }
        }
    }

    private void subscribeTopic(String topicId, SubscriberCallbackInterface callback) throws Exception {
        String result = broker.subscribe(topicId, subscriberName, callback);
        System.out.println("[" + result + "]");
    }

    private void showCurrentSubscriptions() throws Exception {
        List<String> subscriptions = broker.getCurrentSubscriptions(subscriberName);
        if (subscriptions.isEmpty()) {
            System.out.println("No current subscriptions");
        } else {
            for (String subscription : subscriptions) {
                String[] parts = subscription.split("\\s+");
                System.out.println("[" + parts[0] + "] [" + parts[1] + "] [" + parts[2] + "]");
            }
        }
    }

    private void unsubscribeTopic(String topicId) throws Exception {
        String result = broker.unsubscribe(topicId, subscriberName);
        System.out.println("[" + result + "]");
    }

    private void startHeartbeat() {
        new Thread(() -> {
            while (isRunning) {
                try {
                    broker.subscriberHeartbeat(subscriberName);
                    Thread.sleep(3000);
                } catch (Exception e) {
                    System.err.println("Failed to send heartbeat: " + e.getMessage());
                    isRunning = false;
                    break;
                }
            }
        }).start();
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: java -jar subscriber.jar <username> <directory_ip> <directory_port>");
            System.exit(1);
        }
        String subscriberName = args[0];
        String directoryIp = args[1];
        int directoryPort = Integer.parseInt(args[2]);

        SubscriberClient client = new SubscriberClient(subscriberName, directoryIp, directoryPort);
        client.start();
    }
}
