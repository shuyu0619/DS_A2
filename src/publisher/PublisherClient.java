package publisher;

import remote.BrokerInterface;
import remote.DirectoryServiceInterface;
import remote.BrokerInfo;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Scanner;
import java.util.List;
import java.util.Random;

public class PublisherClient {
    private final String publisherName;
    private BrokerInterface broker;
    private final Scanner scanner;
    private volatile boolean isRunning = true;

    public PublisherClient(String publisherName, String directoryIp, int directoryPort) {
        this.publisherName = publisherName;
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


            startHeartbeat();

        } catch (Exception e) {
            System.err.println("Unable to connect to BrokerService: " + e.getMessage());
            System.exit(1);
        }
    }

    public void start() {
        while (isRunning) {
            System.out.println("Please select command: create, publish, show, delete");
            String commandLine = scanner.nextLine().trim();
            String[] parts = commandLine.split("\\s+", 3);
            if (parts.length == 0) {
                System.out.println("error: Invalid command.");
                continue;
            }
            String command = parts[0];
            try {
                switch (command.toLowerCase()) {
                    case "create":
                        if (parts.length != 3) {
                            System.out.println("error: Invalid command format.");
                            break;
                        }
                        createTopic(parts[1], parts[2]);
                        break;
                    case "publish":
                        if (parts.length != 3) {
                            System.out.println("error: Invalid command format.");
                            break;
                        }
                        publishMessage(parts[1], parts[2]);
                        break;
                    case "show":
                        if (parts.length != 2) {
                            System.out.println("error: Invalid command format.");
                            break;
                        }
                        showSubscriberCount(parts[1]);
                        break;
                    case "delete":
                        if (parts.length != 2) {
                            System.out.println("error: Invalid command format.");
                            break;
                        }
                        deleteTopic(parts[1]);
                        break;
                    default:
                        System.out.println("error: Invalid command.");
                }
            } catch (Exception e) {
                System.out.println("error: " + e.getMessage());
            }
        }
        System.out.println("Publisher client exited.");
    }

    private void createTopic(String topicId, String topicName) throws Exception {
        String result = broker.createTopic(topicId, topicName, publisherName);
        System.out.println("[" + result + "]");
    }

    private void publishMessage(String topicId, String message) throws Exception {
        String result = broker.publishMessage(topicId, message, publisherName);
        System.out.println("[" + result + "]");
    }

    private void showSubscriberCount(String topicId) throws Exception {
        List<String> topicInfo = broker.getSubscriberCount(topicId, publisherName);
        if (topicInfo.isEmpty()) {
            System.out.println("[No subscribers]");
        } else {
            for (String info : topicInfo) {
                String[] parts = info.split("\\s+");
                System.out.println("[" + parts[0] + "] [" + parts[1] + "] [" + parts[2] + "]");
            }
        }
    }

    private void deleteTopic(String topicId) throws Exception {
        String result = broker.deleteTopic(topicId, publisherName);
        System.out.println("[" + result + "]");
    }


    private void startHeartbeat() {
        new Thread(() -> {
            while (isRunning) {
                try {
                    broker.publisherHeartbeat(publisherName);
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
            System.out.println("Usage: java -jar publisher.jar <username> <directory_ip> <directory_port>");
            System.exit(1);
        }
        String publisherName = args[0];
        String directoryIp = args[1];
        int directoryPort = Integer.parseInt(args[2]);

        PublisherClient client = new PublisherClient(publisherName, directoryIp, directoryPort);
        client.start();
    }
}
