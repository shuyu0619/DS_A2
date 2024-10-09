package broker;

import remote.SubscriberCallbackInterface;

import java.rmi.RemoteException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class Topic {
    private static final Logger logger = Logger.getLogger(Topic.class.getName());
    private final String topicId;
    private final String topicName;
    private final String publisherName;
    private final ConcurrentHashMap<String, SubscriberCallbackInterface> subscribers;

    public Topic(String topicId, String topicName, String publisherName) {
        this.topicId = topicId;
        this.topicName = topicName;
        this.publisherName = publisherName;
        this.subscribers = new ConcurrentHashMap<>();
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

    public synchronized void addSubscriber(String subscriberName, SubscriberCallbackInterface subscriber) {
        subscribers.put(subscriberName, subscriber);
    }

    public synchronized void removeSubscriber(String subscriberName) {
        subscribers.remove(subscriberName);
    }

    public synchronized int getSubscriberCount() {
        return subscribers.size();
    }

    public synchronized Map<String, SubscriberCallbackInterface> getSubscribers() {
        return subscribers;
    }

    public synchronized void publishMessage(String message) {
        for (Map.Entry<String, SubscriberCallbackInterface> entry : subscribers.entrySet()) {
            String subscriberName = entry.getKey();
            SubscriberCallbackInterface subscriber = entry.getValue();
            try {
                subscriber.notifySubscriber(topicId, topicName, publisherName, message);
            } catch (RemoteException e) {
                subscribers.remove(subscriberName);
            }
        }
    }
}
