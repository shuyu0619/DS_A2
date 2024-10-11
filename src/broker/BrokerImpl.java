package broker;

import remote.BrokerInterface;
import remote.DirectoryServiceInterface;
import remote.BrokerInfo;
import remote.SubscriberCallbackInterface;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.net.InetAddress;

public class BrokerImpl extends UnicastRemoteObject implements BrokerInterface {

    private final String brokerIdentifier;
    private final DirectoryServiceInterface directoryService;

    private final ConcurrentHashMap<String, Topic> topics = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Set<String>> subscriberTopics = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Set<String>> publisherTopics = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Set<String>> topicSubscribersOnBrokers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, BrokerInterface> otherBrokers = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, Long> publisherHeartbeats = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> subscriberHeartbeats = new ConcurrentHashMap<>();

    private List<BrokerInfo> initialBrokerList;

    public BrokerImpl(int currentPort, String directoryIp, int directoryPort) throws RemoteException {
        super();
        try {
            String hostname = InetAddress.getLocalHost().getHostAddress();
            this.brokerIdentifier = hostname + ":" + currentPort;

            Registry directoryRegistry = LocateRegistry.getRegistry(directoryIp, directoryPort);
            directoryService = (DirectoryServiceInterface) directoryRegistry.lookup("DirectoryService");

            initialBrokerList = directoryService.registerAndGetBrokerList(brokerIdentifier, hostname, currentPort);

            startPublisherHeartbeatChecker();
            startSubscriberHeartbeatChecker();

        } catch (Exception e) {
            e.printStackTrace();
            throw new RemoteException("Failed to initialize broker", e);
        }
    }

    public void initializeConnections() {
        try {
            for (BrokerInfo brokerInfo : initialBrokerList) {
                if (!brokerInfo.getBrokerId().equals(brokerIdentifier)) {
                    try {
                        Registry otherRegistry = LocateRegistry.getRegistry(brokerInfo.getIp(), brokerInfo.getPort());
                        String otherServiceName = "BrokerService_" + brokerInfo.getPort();
                        BrokerInterface otherBroker = (BrokerInterface) otherRegistry.lookup(otherServiceName);
                        otherBrokers.put(brokerInfo.getBrokerId(), otherBroker);

                        List<String> otherTopics = otherBroker.getAllTopics();
                        for (String topicInfo : otherTopics) {
                            String[] parts = topicInfo.split("\\s+");
                            String topicId = parts[0];
                            String topicName = parts[1];
                            String publisherName = parts[2];
                            if (!topics.containsKey(topicId)) {
                                topics.put(topicId, new Topic(topicId, topicName, publisherName, this));
                            }
                        }

                        otherBroker.registerNewBroker(brokerIdentifier, InetAddress.getLocalHost().getHostAddress(), getPortFromIdentifier(brokerIdentifier));

                    } catch (Exception e) {
                        System.err.println("Failed to connect to broker " + brokerInfo.getBrokerId() + ": " + e.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private int getPortFromIdentifier(String brokerIdentifier) {
        String[] parts = brokerIdentifier.split(":");
        return Integer.parseInt(parts[1]);
    }

    @Override
    public synchronized void registerNewBroker(String brokerId, String ip, int port) throws RemoteException {
        if (!otherBrokers.containsKey(brokerId) && !brokerIdentifier.equals(brokerId)) {
            try {
                Registry registry = LocateRegistry.getRegistry(ip, port);
                String serviceName = "BrokerService_" + port;
                BrokerInterface broker = (BrokerInterface) registry.lookup(serviceName);
                otherBrokers.put(brokerId, broker);
                System.out.println("Registered new broker: " + brokerId);

                List<String> otherTopics = broker.getAllTopics();
                for (String topicInfo : otherTopics) {
                    String[] parts = topicInfo.split("\\s+");
                    String topicId = parts[0];
                    String topicName = parts[1];
                    String publisherName = parts[2];
                    if (!topics.containsKey(topicId)) {
                        topics.put(topicId, new Topic(topicId, topicName, publisherName, this));
                    }
                }

            } catch (Exception e) {
                System.err.println("Failed to register new broker " + brokerId + ": " + e.getMessage());
            }
        }
    }

    @Override
    public String createTopic(String topicId, String topicName, String publisherName) throws RemoteException {
        checkPublisher(publisherName);

        synchronized (this) {
            if (topics.containsKey(topicId)) {
                return "error: Topic already exists: " + topicId;
            }
            Topic topic = new Topic(topicId, topicName, publisherName, this);
            topics.put(topicId, topic);
            publisherTopics.computeIfAbsent(publisherName, k -> ConcurrentHashMap.newKeySet()).add(topicId);
        }
        synchronizeTopicWithOthers(topicId, topicName, publisherName);
        return "success";
    }

    private void synchronizeTopicWithOthers(String topicId, String topicName, String publisherName) {
        for (BrokerInterface broker : otherBrokers.values()) {
            try {
                broker.synchronizeTopic(topicId, topicName, publisherName);
            } catch (RemoteException e) {
                System.err.println("Failed to synchronize topic: " + e.getMessage());
            }
        }
    }

    @Override
    public synchronized void synchronizeTopic(String topicId, String topicName, String publisherName) throws RemoteException {
        if (!topics.containsKey(topicId)) {
            Topic topic = new Topic(topicId, topicName, publisherName, this);
            topics.put(topicId, topic);
            publisherTopics.computeIfAbsent(publisherName, k -> ConcurrentHashMap.newKeySet()).add(topicId);
        }
    }

    @Override
    public String publishMessage(String topicId, String message, String publisherName) throws RemoteException {
        checkPublisher(publisherName);

        Topic topic;
        synchronized (this) {
            topic = topics.get(topicId);
            if (topic == null) {
                return "error: Topic not found: " + topicId;
            }
            if (!topic.getPublisherName().equals(publisherName)) {
                return "error: Unauthorized publisher: " + publisherName;
            }
        }
        topic.publishMessage(message);
        forwardMessageToOthers(topicId, message, publisherName);
        return "success";
    }

    private void forwardMessageToOthers(String topicId, String message, String publisherName) {
        Set<String> brokersWithSubscribers = topicSubscribersOnBrokers.get(topicId);
        if (brokersWithSubscribers != null) {
            for (String brokerId : brokersWithSubscribers) {
                if (!brokerId.equals(this.brokerIdentifier)) {
                    BrokerInterface broker = otherBrokers.get(brokerId);
                    if (broker != null) {
                        try {
                            broker.forwardMessage(topicId, message, publisherName);
                        } catch (RemoteException e) {
                            System.err.println("Failed to forward message: " + e.getMessage());
                        }
                    }
                }
            }
        }
    }

    @Override
    public void forwardMessage(String topicId, String message, String publisherName) throws RemoteException {
        Topic topic;
        synchronized (this) {
            topic = topics.get(topicId);
            if (topic == null) {
                String topicInfo = getTopicInfoFromOtherBrokers(topicId);
                if (topicInfo != null) {
                    String[] parts = topicInfo.split("\\s+");
                    String topicName = parts[1];
                    String publisher = parts[2];
                    topic = new Topic(topicId, topicName, publisher, this);
                    topics.put(topicId, topic);
                } else {
                    System.err.println("Failed to get topic info for topicId: " + topicId);
                    return;
                }
            }
        }
        topic.publishMessage(message);
    }

    private String getTopicInfoFromOtherBrokers(String topicId) {
        for (BrokerInterface broker : otherBrokers.values()) {
            try {
                String topicInfo = broker.getTopicInfo(topicId);
                if (topicInfo != null) {
                    return topicInfo;
                }
            } catch (RemoteException e) {
                System.err.println("Failed to get topic info from broker: " + e.getMessage());
            }
        }
        return null;
    }

    @Override
    public List<String> listTopics() throws RemoteException {
        Set<String> seenTopics = new HashSet<>();
        List<String> topicList = new ArrayList<>();

        synchronized (this) {
            for (Topic topic : topics.values()) {
                String entry = topic.getTopicId() + " " + topic.getTopicName() + " " + topic.getPublisherName();
                topicList.add(entry);
                seenTopics.add(topic.getTopicId());
            }
        }

        for (BrokerInterface broker : otherBrokers.values()) {
            try {
                List<String> otherTopics = broker.getAllTopics();
                for (String topicInfo : otherTopics) {
                    String[] parts = topicInfo.split("\\s+");
                    String topicId = parts[0];
                    if (!seenTopics.contains(topicId)) {
                        topicList.add(topicInfo);
                        seenTopics.add(topicId);
                    }
                }
            } catch (RemoteException e) {
                System.err.println("Failed to get topics from broker: " + e.getMessage());
            }
        }
        return topicList;
    }

    @Override
    public String subscribe(String topicId, String subscriberName, SubscriberCallbackInterface subscriber) throws RemoteException {
        Topic topic;
        synchronized (this) {
            topic = topics.get(topicId);
            if (topic == null) {
                String topicInfo = getTopicInfoFromOtherBrokers(topicId);
                if (topicInfo != null) {
                    String[] parts = topicInfo.split("\\s+");
                    String topicName = parts[1];
                    String publisherName = parts[2];
                    topic = new Topic(topicId, topicName, publisherName, this);
                    topics.put(topicId, topic);
                } else {
                    return "error: Topic not found: " + topicId;
                }
            }
            topic.addSubscriber(subscriberName, subscriber);
            subscriberTopics.computeIfAbsent(subscriberName, k -> ConcurrentHashMap.newKeySet()).add(topicId);
            topicSubscribersOnBrokers.computeIfAbsent(topicId, k -> ConcurrentHashMap.newKeySet()).add(brokerIdentifier);
        }
        synchronizeSubscriptionWithOthers(topicId, subscriberName, "subscribe");
        return "success";
    }

    @Override
    public void synchronizeSubscription(String topicId, String subscriberName, String action, String brokerId) throws RemoteException {
        synchronized (this) {
            if (action.equals("subscribe")) {
                if (!topics.containsKey(topicId)) {
                    BrokerInterface broker = otherBrokers.get(brokerId);
                    if (broker != null) {
                        try {
                            String topicInfo = broker.getTopicInfo(topicId);
                            if (topicInfo != null) {
                                String[] parts = topicInfo.split("\\s+");
                                String topicName = parts[1];
                                String publisherName = parts[2];
                                Topic topic = new Topic(topicId, topicName, publisherName, this);
                                topics.put(topicId, topic);
                            }
                        } catch (RemoteException e) {
                            System.err.println("Failed to get topic info from broker " + brokerId + ": " + e.getMessage());
                        }
                    }
                }
                topicSubscribersOnBrokers.computeIfAbsent(topicId, k -> ConcurrentHashMap.newKeySet()).add(brokerId);
            } else if (action.equals("unsubscribe")) {
                Set<String> brokersSet = topicSubscribersOnBrokers.get(topicId);
                if (brokersSet != null) {
                    brokersSet.remove(brokerId);
                    if (brokersSet.isEmpty()) {
                        topicSubscribersOnBrokers.remove(topicId);
                    }
                }
            }
        }
    }

    public void removeSubscriberFromBroker(String subscriberName, String topicId) {
        synchronized (this) {
            Set<String> subscribedTopics = subscriberTopics.get(subscriberName);
            if (subscribedTopics != null) {
                subscribedTopics.remove(topicId);
                if (subscribedTopics.isEmpty()) {
                    subscriberTopics.remove(subscriberName);
                }
            }
            Set<String> brokersSet = topicSubscribersOnBrokers.get(topicId);
            if (brokersSet != null) {
                brokersSet.remove(brokerIdentifier);
                if (brokersSet.isEmpty()) {
                    topicSubscribersOnBrokers.remove(topicId);
                }
            }
        }
    }

    public void synchronizeSubscriptionWithOthers(String topicId, String subscriberName, String action) {
        for (Map.Entry<String, BrokerInterface> entry : otherBrokers.entrySet()) {
            String brokerId = entry.getKey();
            BrokerInterface broker = entry.getValue();
            try {
                broker.synchronizeSubscription(topicId, subscriberName, action, brokerIdentifier);
            } catch (RemoteException e) {
                System.err.println("Failed to synchronize subscription with broker " + brokerId + ": " + e.getMessage());
                otherBrokers.remove(brokerId);
            }
        }
    }

    @Override
    public String unsubscribe(String topicId, String subscriberName) throws RemoteException {
        synchronized (this) {
            Topic topic = topics.get(topicId);
            if (topic == null) {
                return "error: Topic not found: " + topicId;
            }
            topic.removeSubscriber(subscriberName);
            removeSubscriberFromBroker(subscriberName, topicId);
        }
        synchronizeSubscriptionWithOthers(topicId, subscriberName, "unsubscribe");
        return "success";
    }

    @Override
    public List<String> getCurrentSubscriptions(String subscriberName) throws RemoteException {
        List<String> subscriptions = new ArrayList<>();
        synchronized (this) {
            Set<String> subscribedTopics = subscriberTopics.get(subscriberName);
            if (subscribedTopics != null) {
                for (String topicId : subscribedTopics) {
                    Topic topic = topics.get(topicId);
                    if (topic != null) {
                        String entry = topic.getTopicId() + " " + topic.getTopicName() + " " + topic.getPublisherName();
                        subscriptions.add(entry);
                    }
                }
            }
        }
        return subscriptions;
    }

    @Override
    public List<String> getSubscriberCount(String topicId, String publisherName) throws RemoteException {
        Topic topic;
        synchronized (this) {
            topic = topics.get(topicId);
            if (topic == null) {
                return Collections.singletonList("error: Topic not found: " + topicId);
            }
            if (!topic.getPublisherName().equals(publisherName)) {
                return Collections.singletonList("error: Unauthorized publisher: " + publisherName);
            }
        }
        int totalSubscribers = topic.getSubscriberCount();
        Set<String> brokersSet = topicSubscribersOnBrokers.get(topicId);
        if (brokersSet != null) {
            for (String brokerId : brokersSet) {
                if (!brokerId.equals(this.brokerIdentifier)) {
                    BrokerInterface broker = otherBrokers.get(brokerId);
                    if (broker != null) {
                        try {
                            totalSubscribers += broker.getLocalSubscriberCount(topicId);
                        } catch (RemoteException e) {
                            System.err.println("Failed to get local subscriber count from broker " + brokerId + ": " + e.getMessage());
                        }
                    }
                }
            }
        }
        String result = topic.getTopicId() + " " + topic.getTopicName() + " " + totalSubscribers;
        return Collections.singletonList(result);
    }

    @Override
    public int getLocalSubscriberCount(String topicId) throws RemoteException {
        synchronized (this) {
            Topic topic = topics.get(topicId);
            return (topic != null) ? topic.getSubscriberCount() : 0;
        }
    }

    @Override
    public String deleteTopic(String topicId, String publisherName) throws RemoteException {
        checkPublisher(publisherName);

        Topic topic;
        synchronized (this) {
            topic = topics.get(topicId);
            if (topic == null) {
                return "error: Topic not found: " + topicId;
            }
            if (!topic.getPublisherName().equals(publisherName)) {
                return "error: Unauthorized publisher: " + publisherName;
            }

            topic.notifySubscribersOfTopicDeletion();
            topics.remove(topicId);
            Set<String> publisherTopicSet = publisherTopics.get(publisherName);
            if (publisherTopicSet != null) {
                publisherTopicSet.remove(topicId);
                if (publisherTopicSet.isEmpty()) {
                    publisherTopics.remove(publisherName);
                }
            }
            for (Set<String> subscribedTopics : subscriberTopics.values()) {
                subscribedTopics.remove(topicId);
            }
            subscriberTopics.entrySet().removeIf(entry -> entry.getValue().isEmpty());
            topicSubscribersOnBrokers.remove(topicId);
        }
        synchronizeTopicDeletionWithOthers(topicId);
        return "success";
    }

    private void synchronizeTopicDeletionWithOthers(String topicId) {
        for (BrokerInterface broker : otherBrokers.values()) {
            try {
                broker.synchronizeTopicDeletion(topicId);
            } catch (RemoteException e) {
                System.err.println("Failed to synchronize topic deletion: " + e.getMessage());
            }
        }
    }

    @Override
    public void synchronizeTopicDeletion(String topicId) throws RemoteException {
        synchronized (this) {
            Topic topic = topics.remove(topicId);
            if (topic != null) {
                String publisherName = topic.getPublisherName();
                Set<String> publisherTopicSet = publisherTopics.get(publisherName);
                if (publisherTopicSet != null) {
                    publisherTopicSet.remove(topicId);
                    if (publisherTopicSet.isEmpty()) {
                        publisherTopics.remove(publisherName);
                    }
                }
                for (Set<String> subscribedTopics : subscriberTopics.values()) {
                    subscribedTopics.remove(topicId);
                }
                subscriberTopics.entrySet().removeIf(entry -> entry.getValue().isEmpty());
                topicSubscribersOnBrokers.remove(topicId);
            }
        }
    }


    @Override
    public List<String> getAllTopics() throws RemoteException {
        List<String> topicList = new ArrayList<>();
        synchronized (this) {
            for (Topic topic : topics.values()) {
                String entry = topic.getTopicId() + " " + topic.getTopicName() + " " + topic.getPublisherName();
                topicList.add(entry);
            }
        }
        return topicList;
    }

    @Override
    public String getTopicInfo(String topicId) throws RemoteException {
        synchronized (this) {
            Topic topic = topics.get(topicId);
            if (topic != null) {
                return topic.getTopicId() + " " + topic.getTopicName() + " " + topic.getPublisherName();
            } else {
                return null;
            }
        }
    }

    @Override
    public String handlePublisherCrash(String publisherName) throws RemoteException {
        System.err.println("Handling publisher crash for: " + publisherName);

        synchronized (this) {
            publisherHeartbeats.remove(publisherName);

            Set<String> publisherTopicIds = publisherTopics.remove(publisherName);
            if (publisherTopicIds != null) {
                for (String topicId : publisherTopicIds) {
                    Topic topic = topics.remove(topicId);
                    if (topic != null) {
                        topic.notifySubscribersOfTopicDeletion();
                        for (Set<String> subscribedTopics : subscriberTopics.values()) {
                            subscribedTopics.remove(topicId);
                        }
                        subscriberTopics.entrySet().removeIf(entry -> entry.getValue().isEmpty());
                        topicSubscribersOnBrokers.remove(topicId);
                    }
                }
            }
        }

        synchronizePublisherCrashWithOthers(publisherName);

        return "success";
    }

    private void synchronizePublisherCrashWithOthers(String publisherName) {
        for (BrokerInterface broker : otherBrokers.values()) {
            try {
                broker.handlePublisherCrashLocal(publisherName);
            } catch (RemoteException e) {
                System.err.println("Failed to synchronize publisher crash: " + e.getMessage());
            }
        }
    }

    @Override
    public void handlePublisherCrashLocal(String publisherName) throws RemoteException {
        System.err.println("Handling local publisher crash for: " + publisherName);

        synchronized (this) {
            publisherHeartbeats.remove(publisherName);

            Set<String> publisherTopicIds = publisherTopics.remove(publisherName);
            if (publisherTopicIds != null) {
                for (String topicId : publisherTopicIds) {
                    Topic topic = topics.remove(topicId);
                    if (topic != null) {
                        topic.notifySubscribersOfTopicDeletion();
                        for (Set<String> subscribedTopics : subscriberTopics.values()) {
                            subscribedTopics.remove(topicId);
                        }
                        subscriberTopics.entrySet().removeIf(entry -> entry.getValue().isEmpty());
                        topicSubscribersOnBrokers.remove(topicId);
                    }
                }
            }
        }
    }

    private void checkPublisher(String publisherName) throws RemoteException {
        Long lastHeartbeat = publisherHeartbeats.get(publisherName);
        if (lastHeartbeat != null) {
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastHeartbeat <= 6000) {
                return;
            }
        }

        handlePublisherCrash(publisherName);
        throw new RemoteException("Publisher " + publisherName + " is not available.");
    }

    @Override
    public void publisherHeartbeat(String publisherName) throws RemoteException {
        publisherHeartbeats.put(publisherName, System.currentTimeMillis());
    }

    private void startPublisherHeartbeatChecker() {
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(3000);
                    long currentTime = System.currentTimeMillis();
                    for (Map.Entry<String, Long> entry : publisherHeartbeats.entrySet()) {
                        String publisherName = entry.getKey();
                        long lastHeartbeat = entry.getValue();
                        if (currentTime - lastHeartbeat > 6000) {
                            System.out.println("Publisher " + publisherName + " has crashed or disconnected.");
                            handlePublisherCrash(publisherName);
                            publisherHeartbeats.remove(publisherName);
                        }
                    }
                } catch (InterruptedException e) {
                    System.err.println("Publisher heartbeat checker interrupted: " + e.getMessage());
                    break;
                } catch (RemoteException e) {
                    System.err.println("Failed to handle publisher crash: " + e.getMessage());
                }
            }
        }).start();
    }

    @Override
    public void subscriberHeartbeat(String subscriberName) throws RemoteException {
        subscriberHeartbeats.put(subscriberName, System.currentTimeMillis());
    }

    private void startSubscriberHeartbeatChecker() {
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(3000); // test every 3 seconds
                    long currentTime = System.currentTimeMillis();
                    for (Map.Entry<String, Long> entry : subscriberHeartbeats.entrySet()) {
                        String subscriberName = entry.getKey();
                        long lastHeartbeat = entry.getValue();
                        if (currentTime - lastHeartbeat > 6000) {
                            System.out.println("Subscriber " + subscriberName + " has crashed or disconnected.");
                            handleSubscriberCrash(subscriberName);
                            subscriberHeartbeats.remove(subscriberName);
                        }
                    }
                } catch (InterruptedException e) {
                    System.err.println("Subscriber heartbeat checker interrupted: " + e.getMessage());
                    break;
                }
            }
        }).start();
    }

    private void handleSubscriberCrash(String subscriberName) {

        Set<String> subscribedTopics;
        synchronized (this) {
            subscribedTopics = subscriberTopics.remove(subscriberName);
            if (subscribedTopics != null) {
                for (String topicId : subscribedTopics) {

                    Topic topic = topics.get(topicId);
                    if (topic != null) {
                        topic.removeSubscriber(subscriberName);
                    }

                    Set<String> brokersSet = topicSubscribersOnBrokers.get(topicId);
                    if (brokersSet != null) {
                        brokersSet.remove(brokerIdentifier);
                        if (brokersSet.isEmpty()) {
                            topicSubscribersOnBrokers.remove(topicId);
                        }
                    }
                }
            }
        }

        if (subscribedTopics != null) {
            for (String topicId : subscribedTopics) {
                synchronizeSubscriptionWithOthers(topicId, subscriberName, "unsubscribe");
            }
        }
        System.out.println("Handled subscriber crash for " + subscriberName);
    }
}
