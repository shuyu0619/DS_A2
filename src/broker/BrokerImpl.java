package broker;

import remote.BrokerInterface;
import remote.SubscriberCallbackInterface;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class BrokerImpl extends UnicastRemoteObject implements BrokerInterface {

    private ConcurrentHashMap<String, Topic> topics = new ConcurrentHashMap<>();

    public BrokerImpl() throws RemoteException {
        super();
    }

    @Override
    public synchronized void createTopic(String topicId, String topicName, String publisherName) throws RemoteException {
        if (topics.containsKey(topicId)) {
            throw new RemoteException("Topic already exists: " + topicId);
        } else {
            topics.put(topicId, new Topic(topicId, topicName, publisherName));
            System.out.println("Topic created: " + topicId);
        }
    }

    @Override
    public synchronized void publishMessage(String topicId, String message, String publisherName) throws RemoteException {
        Topic topic = topics.get(topicId);
        if (topic != null) {
            // Check if the publisher is authorized to publish to this topic
            if (!topic.getPublisherName().equals(publisherName)) {
                throw new RemoteException("Unauthorized publisher: " + publisherName);
            }
            topic.publishMessage(message);
            System.out.println("Message published to topic: " + topicId);
        } else {
            throw new RemoteException("Topic not found: " + topicId);
        }
    }

    @Override
    public List<String> listTopics() throws RemoteException {
        return topics.values().stream()
                .map(topic -> topic.getTopicId() + ": " + topic.getTopicName())
                .collect(Collectors.toList());
    }

    @Override
    public synchronized void subscribe(String topicId, String subscriberName, SubscriberCallbackInterface subscriber) throws RemoteException {
        Topic topic = topics.get(topicId);
        if (topic == null) {
            throw new RemoteException("Topic not found: " + topicId);
        }
        topic.addSubscriber(subscriberName, subscriber);
        System.out.println("Subscriber " + subscriberName + " subscribed to topic: " + topicId);
    }

    @Override
    public synchronized void unsubscribe(String topicId, String subscriberName) throws RemoteException {
        Topic topic = topics.get(topicId);
        if (topic == null) {
            throw new RemoteException("Topic not found: " + topicId);
        }
        topic.removeSubscriber(subscriberName);
        System.out.println("Subscriber " + subscriberName + " unsubscribed from topic: " + topicId);
    }
}
