package broker;

import remote.SubscriberCallbackInterface;

import java.rmi.RemoteException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Topic {
    private final String topicId;
    private final String topicName;
    private final String publisherName;
    private final ConcurrentHashMap<String, SubscriberCallbackInterface> subscribers;


    private final BrokerImpl broker;

    public Topic(String topicId, String topicName, String publisherName, BrokerImpl broker) {
        this.topicId = topicId;
        this.topicName = topicName;
        this.publisherName = publisherName;
        this.subscribers = new ConcurrentHashMap<>();
        this.broker = broker;
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
        Iterator<Map.Entry<String, SubscriberCallbackInterface>> iterator = subscribers.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, SubscriberCallbackInterface> entry = iterator.next();
            String subscriberName = entry.getKey();
            SubscriberCallbackInterface subscriber = entry.getValue();
            try {
                subscriber.notifySubscriber(topicId, topicName, publisherName, message);
            } catch (RemoteException e) {
                System.err.println("Subscriber " + subscriberName + " is unreachable. Removing from subscriber list.");
                iterator.remove();


                broker.removeSubscriberFromBroker(subscriberName, topicId);


                broker.synchronizeSubscriptionWithOthers(topicId, subscriberName, "unsubscribe");
            }
        }
    }


    public synchronized void notifySubscribersOfTopicDeletion() {
        String message = "Topic " + topicName + " (ID: " + topicId + ") has been deleted.";
        Iterator<Map.Entry<String, SubscriberCallbackInterface>> iterator = subscribers.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, SubscriberCallbackInterface> entry = iterator.next();
            String subscriberName = entry.getKey();
            SubscriberCallbackInterface subscriber = entry.getValue();
            try {
                subscriber.notifySubscriber(topicId, topicName, publisherName, message);
            } catch (RemoteException e) {
                System.err.println("Subscriber " + subscriberName + " is unreachable during topic deletion. Removing from subscriber list.");
                iterator.remove();


                broker.removeSubscriberFromBroker(subscriberName, topicId);


                broker.synchronizeSubscriptionWithOthers(topicId, subscriberName, "unsubscribe");
            }
        }
    }
}
