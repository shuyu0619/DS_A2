package broker;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import subscriber.SubscriberCallbackInterface;

public class Topic {
    private String topicId;
    private String topicName;
    private String publisherName;
    private Set<SubscriberCallbackInterface> subscribers = new CopyOnWriteArraySet<>();

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

    public Set<SubscriberCallbackInterface> getSubscribers() {
        return subscribers;
    }

    public void addSubscriber(SubscriberCallbackInterface subscriber) {
        subscribers.add(subscriber);
    }

    public void removeSubscriber(SubscriberCallbackInterface subscriber) {
        subscribers.remove(subscriber);
    }

    public void publishMessage(String message) {
        for (SubscriberCallbackInterface subscriber : subscribers) {
            try {
                subscriber.notifySubscriber(message);
            } catch (Exception e) {
                System.err.println("Error notifying subscriber: " + e.getMessage());
                subscribers.remove(subscriber); // remove subscriber if an error occurs
            }
        }
    }

}
