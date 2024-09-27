package broker;

import remote.BrokerInterface;
import remote.SubscriberCallbackInterface;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * BrokerImpl implements the BrokerInterface.
 * It manages topics, publishers, subscribers, and communication with other brokers.
 */
public class BrokerImpl extends UnicastRemoteObject implements BrokerInterface {
    private static final Logger logger = Logger.getLogger(BrokerImpl.class.getName());

    // Stores topics with their IDs as keys
    private final ConcurrentHashMap<String, Topic> topics = new ConcurrentHashMap<>();

    // Maps subscriber names to the set of topic IDs they are subscribed to
    private final ConcurrentHashMap<String, Set<String>> subscriberTopics = new ConcurrentHashMap<>();

    // Maps publisher names to the set of topic IDs they have created
    private final ConcurrentHashMap<String, Set<String>> publisherTopics = new ConcurrentHashMap<>();

    // Tracks which brokers have subscribers for each topic
    private final ConcurrentHashMap<String, Set<String>> topicSubscribersOnBrokers = new ConcurrentHashMap<>();

    // Stores references to other brokers in the network
    private final ConcurrentHashMap<String, BrokerInterface> otherBrokers = new ConcurrentHashMap<>();

    // Unique identifier for this broker (e.g., "localhost:5000")
    private String brokerIdentifier;

    public BrokerImpl() throws RemoteException {
        super();
        logger.info("BrokerImpl initialized.");
    }

    public void setBrokerIdentifier(String brokerIdentifier) {
        this.brokerIdentifier = brokerIdentifier;
        logger.info("Broker Identifier set to: " + brokerIdentifier);
    }

    @Override
    public String getBrokerIdentifier() throws RemoteException {
        return brokerIdentifier;
    }

    // Adds a reference to another broker in the network
    public synchronized void addOtherBroker(BrokerInterface broker) {
        if (broker != null) {
            try {
                String brokerId = broker.getBrokerIdentifier();
                if (!brokerId.equals(this.brokerIdentifier) && !otherBrokers.containsKey(brokerId)) {
                    otherBrokers.put(brokerId, broker);
                    logger.info("Added new broker to the network: " + brokerId);
                }
            } catch (RemoteException e) {
                logger.log(Level.SEVERE, "Failed to get broker identifier: " + e.getMessage(), e);
            }
        }
    }

    // Checks if this broker knows about another broker
    public synchronized boolean hasOtherBroker(String brokerId) {
        return otherBrokers.containsKey(brokerId);
    }

    // Publisher creates a new topic
    @Override
    public synchronized String createTopic(String topicId, String topicName, String publisherName) throws RemoteException {
        if (topics.containsKey(topicId)) {
            logger.warning("Attempt to create duplicate topic: " + topicId + " by publisher " + publisherName);
            throw new RemoteException("Topic already exists: " + topicId);
        }
        // Create a new topic and store it
        Topic topic = new Topic(topicId, topicName, publisherName);
        topics.put(topicId, topic);

        // Update the publisher's topic list
        publisherTopics.computeIfAbsent(publisherName, k -> ConcurrentHashMap.newKeySet()).add(topicId);
        logger.info("Topic created: " + topicId + " by publisher " + publisherName);

        // Synchronize the new topic with other brokers
        synchronizeTopicWithOthers(topicId, topicName, publisherName);

        return "success";
    }

    // Synchronize the new topic with other brokers
    private void synchronizeTopicWithOthers(String topicId, String topicName, String publisherName) {
        for (BrokerInterface broker : otherBrokers.values()) {
            try {
                broker.synchronizeTopic(topicId, topicName, publisherName);
                logger.info("Synchronized topic " + topicId + " with broker: " + broker.getBrokerIdentifier());
            } catch (RemoteException e) {
                logger.log(Level.SEVERE, "Failed to synchronize topic with broker: " + e.getMessage(), e);
            }
        }
    }

    // Method called by other brokers to synchronize topic creation
    @Override
    public synchronized void synchronizeTopic(String topicId, String topicName, String publisherName) throws RemoteException {
        if (!topics.containsKey(topicId)) {
            Topic topic = new Topic(topicId, topicName, publisherName);
            topics.put(topicId, topic);
            publisherTopics.computeIfAbsent(publisherName, k -> ConcurrentHashMap.newKeySet()).add(topicId);
            logger.info("Synchronized topic: " + topicId + " from other broker");
        }
    }

    // Publisher publishes a message to a topic
    @Override
    public synchronized String publishMessage(String topicId, String message, String publisherName) throws RemoteException {
        Topic topic = topics.get(topicId);
        if (topic == null) {
            throw new RemoteException("Topic not found: " + topicId);
        }
        if (!topic.getPublisherName().equals(publisherName)) {
            throw new RemoteException("Unauthorized: Publisher " + publisherName + " cannot publish to topic " + topicId);
        }
        // Publish the message to local subscribers
        topic.publishMessage(message);
        logger.info("Message published to topic " + topicId + " by publisher " + publisherName);

        // Forward the message to other brokers
        forwardMessageToOthers(topicId, message, publisherName);

        return "success";
    }

    // Forward the message to other brokers that have subscribers for this topic
    private void forwardMessageToOthers(String topicId, String message, String publisherName) {
        Set<String> brokersWithSubscribers = topicSubscribersOnBrokers.get(topicId);
        if (brokersWithSubscribers != null) {
            for (String brokerId : brokersWithSubscribers) {
                if (!brokerId.equals(this.brokerIdentifier)) {
                    BrokerInterface broker = otherBrokers.get(brokerId);
                    if (broker != null) {
                        try {
                            broker.forwardMessage(topicId, message, publisherName);
                            logger.info("Forwarded message to broker " + brokerId);
                        } catch (RemoteException e) {
                            logger.log(Level.SEVERE, "Failed to forward message to broker " + brokerId + ": " + e.getMessage(), e);
                        }
                    }
                }
            }
        }
    }

    // Method called by other brokers to receive forwarded messages
    @Override
    public synchronized void forwardMessage(String topicId, String message, String publisherName) throws RemoteException {
        Topic topic = topics.get(topicId);
        if (topic != null) {
            topic.publishMessage(message);
            logger.info("Received forwarded message for topic " + topicId + " from publisher " + publisherName);
        }
    }

    // Lists all available topics
    @Override
    public synchronized List<String> listTopics() throws RemoteException {
        List<String> topicList = new ArrayList<>();
        for (Topic topic : topics.values()) {
            topicList.add(topic.getTopicId() + " " + topic.getTopicName() + " " + topic.getPublisherName());
        }
        logger.info("Listing all topics.");
        return topicList;
    }

    // Subscriber subscribes to a topic
    @Override
    public synchronized String subscribe(String topicId, String subscriberName, SubscriberCallbackInterface subscriber) throws RemoteException {
        Topic topic = topics.get(topicId);
        if (topic == null) {
            throw new RemoteException("Topic not found: " + topicId);
        }
        // Add subscriber to the topic
        topic.addSubscriber(subscriberName, subscriber);

        // Update subscriber's topic list
        subscriberTopics.computeIfAbsent(subscriberName, k -> ConcurrentHashMap.newKeySet()).add(topicId);
        logger.info("Subscriber " + subscriberName + " subscribed to topic " + topicId);

        // Update the list of brokers that have subscribers for this topic
        topicSubscribersOnBrokers.computeIfAbsent(topicId, k -> ConcurrentHashMap.newKeySet()).add(brokerIdentifier);

        // Synchronize subscription with other brokers
        synchronizeSubscriptionWithOthers(topicId, subscriberName, "subscribe");

        return "success";
    }

    // Synchronize subscription information with other brokers
    private void synchronizeSubscriptionWithOthers(String topicId, String subscriberName, String action) {
        for (BrokerInterface broker : otherBrokers.values()) {
            try {
                broker.synchronizeSubscription(topicId, subscriberName, action, brokerIdentifier);
                logger.info("Synchronized subscription (" + action + ") with broker: " + broker.getBrokerIdentifier());
            } catch (RemoteException e) {
                logger.log(Level.SEVERE, "Failed to synchronize subscription with broker: " + e.getMessage(), e);
            }
        }
    }

    // Method called by other brokers to synchronize subscription information
    @Override
    public synchronized void synchronizeSubscription(String topicId, String subscriberName, String action, String brokerId) throws RemoteException {
        if (action.equals("subscribe")) {
            topicSubscribersOnBrokers.computeIfAbsent(topicId, k -> ConcurrentHashMap.newKeySet()).add(brokerId);
            logger.info("Synchronized subscription: Subscriber " + subscriberName + " subscribed to topic " + topicId + " on broker " + brokerId);
        } else if (action.equals("unsubscribe")) {
            Set<String> brokersSet = topicSubscribersOnBrokers.get(topicId);
            if (brokersSet != null) {
                brokersSet.remove(brokerId);
                if (brokersSet.isEmpty()) {
                    topicSubscribersOnBrokers.remove(topicId);
                }
            }
            logger.info("Synchronized unsubscription: Subscriber " + subscriberName + " unsubscribed from topic " + topicId + " on broker " + brokerId);
        }
    }

    // Subscriber unsubscribes from a topic
    @Override
    public synchronized String unsubscribe(String topicId, String subscriberName) throws RemoteException {
        Topic topic = topics.get(topicId);
        if (topic == null) {
            throw new RemoteException("Topic not found: " + topicId);
        }
        // Remove subscriber from the topic
        topic.removeSubscriber(subscriberName);

        // Update subscriber's topic list
        Set<String> subscribedTopics = subscriberTopics.get(subscriberName);
        if (subscribedTopics != null) {
            subscribedTopics.remove(topicId);
            if (subscribedTopics.isEmpty()) {
                subscriberTopics.remove(subscriberName);
            }
        }
        logger.info("Subscriber " + subscriberName + " unsubscribed from topic " + topicId);

        // Update the list of brokers that have subscribers for this topic
        Set<String> brokersSet = topicSubscribersOnBrokers.get(topicId);
        if (brokersSet != null) {
            brokersSet.remove(brokerIdentifier);
            if (brokersSet.isEmpty()) {
                topicSubscribersOnBrokers.remove(topicId);
            }
        }

        // Synchronize unsubscription with other brokers
        synchronizeSubscriptionWithOthers(topicId, subscriberName, "unsubscribe");

        return "success";
    }

    // Publisher requests subscriber count for a topic
    @Override
    public synchronized List<String> getSubscriberCount(String topicId, String publisherName) throws RemoteException {
        List<String> result = new ArrayList<>();
        Topic topic = topics.get(topicId);
        if (topic == null) {
            throw new RemoteException("Topic not found: " + topicId);
        }
        if (!topic.getPublisherName().equals(publisherName)) {
            throw new RemoteException("Unauthorized: Publisher " + publisherName + " cannot access topic " + topicId);
        }
        int totalSubscribers = topic.getSubscriberCount();

        // Sum subscriber counts from other brokers
        Set<String> brokersSet = topicSubscribersOnBrokers.get(topicId);
        if (brokersSet != null) {
            for (String brokerId : brokersSet) {
                if (!brokerId.equals(this.brokerIdentifier)) {
                    BrokerInterface broker = otherBrokers.get(brokerId);
                    if (broker != null) {
                        try {
                            totalSubscribers += broker.getLocalSubscriberCount(topicId);
                        } catch (RemoteException e) {
                            logger.log(Level.SEVERE, "Failed to get local subscriber count from broker " + brokerId + ": " + e.getMessage(), e);
                        }
                    }
                }
            }
        }

        result.add(topic.getTopicId() + " " + topic.getTopicName() + " " + totalSubscribers);
        logger.info("Subscriber count requested for topic " + topicId);
        return result;
    }

    // Returns the local subscriber count for a topic
    @Override
    public synchronized int getLocalSubscriberCount(String topicId) throws RemoteException {
        Topic topic = topics.get(topicId);
        return (topic != null) ? topic.getSubscriberCount() : 0;
    }

    // Publisher deletes a topic
    @Override
    public synchronized String deleteTopic(String topicId, String publisherName) throws RemoteException {
        Topic topic = topics.get(topicId);
        if (topic == null) {
            throw new RemoteException("Topic not found: " + topicId);
        }
        if (!topic.getPublisherName().equals(publisherName)) {
            throw new RemoteException("Unauthorized: Publisher " + publisherName + " cannot delete topic " + topicId);
        }

        // Notify local subscribers about topic deletion
        notifySubscribersOfTopicDeletion(topic);

        // Remove topic and update publisher's topic list
        topics.remove(topicId);
        publisherTopics.get(publisherName).remove(topicId);
        if (publisherTopics.get(publisherName).isEmpty()) {
            publisherTopics.remove(publisherName);
        }

        // Remove topic from subscribers' lists
        for (Set<String> subscribedTopics : subscriberTopics.values()) {
            subscribedTopics.remove(topicId);
        }
        subscriberTopics.entrySet().removeIf(entry -> entry.getValue().isEmpty());

        // Remove topic from topicSubscribersOnBrokers
        topicSubscribersOnBrokers.remove(topicId);

        logger.info("Topic deleted: " + topicId + " by publisher " + publisherName);

        // Synchronize topic deletion with other brokers
        synchronizeTopicDeletionWithOthers(topicId);

        return "success";
    }

    // Synchronize topic deletion with other brokers
    private void synchronizeTopicDeletionWithOthers(String topicId) {
        for (BrokerInterface broker : otherBrokers.values()) {
            try {
                broker.synchronizeTopicDeletion(topicId);
                logger.info("Synchronized topic deletion with broker: " + broker.getBrokerIdentifier());
            } catch (RemoteException e) {
                logger.log(Level.SEVERE, "Failed to synchronize topic deletion with broker: " + e.getMessage(), e);
            }
        }
    }

    // Method called by other brokers to synchronize topic deletion
    @Override
    public synchronized void synchronizeTopicDeletion(String topicId) throws RemoteException {
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

            // Remove topic from subscribers' lists
            for (Set<String> subscribedTopics : subscriberTopics.values()) {
                subscribedTopics.remove(topicId);
            }
            subscriberTopics.entrySet().removeIf(entry -> entry.getValue().isEmpty());

            // Remove topic from topicSubscribersOnBrokers
            topicSubscribersOnBrokers.remove(topicId);

            logger.info("Synchronized topic deletion: " + topicId);
        }
    }

    // Notifies local subscribers that the topic has been deleted
    private void notifySubscribersOfTopicDeletion(Topic topic) {
        String message = "Topic " + topic.getTopicName() + " (ID: " + topic.getTopicId() + ") has been deleted.";
        topic.getSubscribers().forEach((subscriberName, subscriber) -> {
            try {
                subscriber.notifySubscriber(topic.getTopicId(), topic.getTopicName(), topic.getPublisherName(), message);
                logger.info("Notified subscriber " + subscriberName + " of topic deletion.");
            } catch (RemoteException e) {
                logger.log(Level.SEVERE, "Failed to notify subscriber " + subscriberName + ": " + e.getMessage(), e);
            }
        });
    }

    // Returns the current subscriptions of a subscriber
    @Override
    public synchronized List<String> getCurrentSubscriptions(String subscriberName) throws RemoteException {
        List<String> subscriptions = new ArrayList<>();
        Set<String> subscribedTopics = subscriberTopics.get(subscriberName);
        if (subscribedTopics != null && !subscribedTopics.isEmpty()) {
            for (String topicId : subscribedTopics) {
                Topic topic = topics.get(topicId);
                if (topic != null) {
                    subscriptions.add(topic.getTopicId() + " " + topic.getTopicName() + " " + topic.getPublisherName());
                }
            }
        }
        logger.info("Current subscriptions requested for subscriber " + subscriberName);
        return subscriptions;
    }

    // Handles publisher crash by deleting their topics and notifying subscribers
    @Override
    public synchronized String handlePublisherCrash(String publisherName) throws RemoteException {
        Set<String> publisherTopicIds = publisherTopics.remove(publisherName);
        if (publisherTopicIds != null) {
            for (String topicId : publisherTopicIds) {
                Topic topic = topics.remove(topicId);
                if (topic != null) {
                    notifySubscribersOfTopicDeletion(topic);

                    // Remove topic from subscribers' lists
                    for (Set<String> subscribedTopics : subscriberTopics.values()) {
                        subscribedTopics.remove(topicId);
                    }
                    subscriberTopics.entrySet().removeIf(entry -> entry.getValue().isEmpty());

                    // Remove topic from topicSubscribersOnBrokers
                    topicSubscribersOnBrokers.remove(topicId);
                }
            }
        }
        logger.info("Handled crash of publisher " + publisherName);

        // Synchronize publisher crash handling with other brokers
        synchronizePublisherCrashWithOthers(publisherName);

        return "success";
    }

    // Synchronize publisher crash with other brokers
    private void synchronizePublisherCrashWithOthers(String publisherName) {
        for (BrokerInterface broker : otherBrokers.values()) {
            try {
                broker.handlePublisherCrash(publisherName);
                logger.info("Synchronized publisher crash with broker: " + broker.getBrokerIdentifier());
            } catch (RemoteException e) {
                logger.log(Level.SEVERE, "Failed to synchronize publisher crash with broker: " + e.getMessage(), e);
            }
        }
    }

    // Returns a list of known brokers in the network
    @Override
    public synchronized List<String> getKnownBrokers() throws RemoteException {
        List<String> knownBrokerIdentifiers = new ArrayList<>(otherBrokers.keySet());
        knownBrokerIdentifiers.add(brokerIdentifier);
        return knownBrokerIdentifiers;
    }
}
