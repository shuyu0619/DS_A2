package broker;

import remote.SubscriberCallbackInterface;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Topic {
    private String topicId;
    private String topicName;
    private String publisherName;
    private Map<String, SubscriberCallbackInterface> subscribers = new ConcurrentHashMap<>();

    public Topic(String topicId, String topicName, String publisherName) {
        this.topicId = topicId;
        this.topicName = topicName;
        this.publisherName = publisherName;
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

    public void addSubscriber(String subscriberName, SubscriberCallbackInterface subscriber) {
        subscribers.put(subscriberName, subscriber);
    }

    public void removeSubscriber(String subscriberName) {
        subscribers.remove(subscriberName);
    }

    public void publishMessage(String message) {
        subscribers.forEach((name, subscriber) -> {
            try {
                subscriber.notifySubscriber(topicId, topicName, publisherName, message);
            } catch (Exception e) {
                System.err.println("Error notifying subscriber " + name + ": " + e.getMessage());
                subscribers.remove(name);
            }
        });
    }

    public int getSubscriberCount() {
        return subscribers.size();
    }
}