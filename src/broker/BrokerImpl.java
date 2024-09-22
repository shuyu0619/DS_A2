package broker;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import subscriber.SubscriberCallbackInterface;

public class BrokerImpl extends UnicastRemoteObject implements BrokerInterface {
    private ConcurrentHashMap<String, Topic> topics = new ConcurrentHashMap<>();

    public BrokerImpl() throws RemoteException {
        super();
    }

    @Override
    public void createTopic(String topicId, String topicName, String publisherName) throws RemoteException {
        if (topics.containsKey(topicId)) {
            System.out.println("Topic already exists: " + topicId);
        } else {
            topics.put(topicId, new Topic(topicId, topicName, publisherName));
            System.out.println("Topic created: " + topicId);
        }
    }

    @Override
    public void publishMessage(String topicId, String message, String publisherName) throws RemoteException {
        Topic topic = topics.get(topicId);
        if (topic == null) {
            throw new RemoteException("Topic not found: " + topicId);
        }
        // Check if the publisher is authorized to publish to the topic
        if (!topic.getPublisherName().equals(publisherName)) {
            throw new RemoteException("Unauthorized publisher: " + publisherName);
        }
        topic.publishMessage(message);
        System.out.println("Message published to topic: " + topicId);
    }


    @Override
    public List<String> listTopics() throws RemoteException {
        return topics.values().stream().map(Topic::getTopicName).collect(Collectors.toList());
    }

    @Override
    public void subscribe(String topicId, String subscriberName, SubscriberCallbackInterface subscriber) throws RemoteException {
        Topic topic = topics.get(topicId);
        if (topic == null) {
            System.out.println("Topic not found: " + topicId);
            return;
        }
        topic.addSubscriber(subscriber);
        System.out.println("Subscriber " + subscriberName + " subscribed to topic: " + topicId);
    }
}
