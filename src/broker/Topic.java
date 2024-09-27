package broker;

import remote.SubscriberCallbackInterface;

import java.rmi.RemoteException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * Topic represents a topic in the publish-subscribe system.
 * It manages subscribers and handles message distribution.
 */
public class Topic {
    private static final Logger logger = Logger.getLogger(Topic.class.getName());

    private final String topicId;
    private final String topicName;
    private final String publisherName;
    // Stores subscribers with their names as keys
    private final ConcurrentHashMap<String, SubscriberCallbackInterface> subscribers;

    public Topic(String topicId, String topicName, String publisherName) {
        this.topicId = topicId;
        this.topicName = topicName;
        this.publisherName = publisherName;
        this.subscribers = new ConcurrentHashMap<>();
        logger.info("Topic created: " + topicId + " - " + topicName + " by " + publisherName);
    }

    public String getTopicId() {
        return topicId;
    }

    public String getTopicName() {
        return topicName;
    }

    public String getPublisherName() {
        return publisherName;
    }

    // Adds a subscriber to this topic
    public synchronized void addSubscriber(String subscriberName, SubscriberCallbackInterface subscriber) {
        subscribers.put(subscriberName, subscriber);
        logger.info("Subscriber " + subscriberName + " added to topic " + topicId);
    }

    // Removes a subscriber from this topic
    public synchronized void removeSubscriber(String subscriberName) {
        subscribers.remove(subscriberName);
        logger.info("Subscriber " + subscriberName + " removed from topic " + topicId);
    }

    // Gets the number of subscribers for this topic
    public synchronized int getSubscriberCount() {
        return subscribers.size();
    }

    // Gets the map of subscribers
    public synchronized Map<String, SubscriberCallbackInterface> getSubscribers() {
        return subscribers;
    }

    // Publishes a message to all subscribers of this topic
    public synchronized void publishMessage(String message) {
        logger.info("Publishing message to topic " + topicId + ": " + message);
        for (Map.Entry<String, SubscriberCallbackInterface> entry : subscribers.entrySet()) {
            String subscriberName = entry.getKey();
            SubscriberCallbackInterface subscriber = entry.getValue();
            try {
                subscriber.notifySubscriber(topicId, topicName, publisherName, message);
                logger.info("Message sent to subscriber " + subscriberName);
            } catch (RemoteException e) {
                logger.log(Level.SEVERE, "Failed to notify subscriber {0}: {1}", new Object[]{subscriberName, e.getMessage()});
                // Remove the subscriber due to failure
                subscribers.remove(subscriberName);
            }
        }
    }
}
