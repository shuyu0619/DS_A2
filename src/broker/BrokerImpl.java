package broker;

import remote.BrokerInterface;
import remote.SubscriberCallbackInterface;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class BrokerImpl extends UnicastRemoteObject implements BrokerInterface {

    private ConcurrentHashMap<String, Topic> topics = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Set<String>> subscriberTopics = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Set<String>> publisherTopics = new ConcurrentHashMap<>();

    public BrokerImpl() throws RemoteException {
        super();
    }

    @Override
    public synchronized void createTopic(String topicId, String topicName, String publisherName) throws RemoteException {
        if (topics.containsKey(topicId)) {
            throw new RemoteException("Topic already exists: " + topicId);
        }
        topics.put(topicId, new Topic(topicId, topicName, publisherName));
        publisherTopics.computeIfAbsent(publisherName, k -> new HashSet<>()).add(topicId);
    }

    @Override
    public synchronized void publishMessage(String topicId, String message, String publisherName) throws RemoteException {
        Topic topic = topics.get(topicId);
        if (topic == null) {
            throw new RemoteException("Topic not found: " + topicId);
        }
        if (!topic.getPublisherName().equals(publisherName)) {
            throw new RemoteException("Unauthorized: Publisher " + publisherName + " cannot publish to topic " + topicId);
        }
        topic.publishMessage(message);
    }

    @Override
    public List<String> listTopics() throws RemoteException {
        return topics.values().stream()
                .map(topic -> topic.getTopicId() + " " + topic.getTopicName() + " " + topic.getPublisherName())
                .collect(Collectors.toList());
    }

    @Override
    public synchronized void subscribe(String topicId, String subscriberName, SubscriberCallbackInterface subscriber) throws RemoteException {
        Topic topic = topics.get(topicId);
        if (topic == null) {
            throw new RemoteException("Topic not found: " + topicId);
        }
        topic.addSubscriber(subscriberName, subscriber);
        subscriberTopics.computeIfAbsent(subscriberName, k -> new HashSet<>()).add(topicId);
    }

    @Override
    public synchronized void unsubscribe(String topicId, String subscriberName) throws RemoteException {
        Topic topic = topics.get(topicId);
        if (topic == null) {
            throw new RemoteException("Topic not found: " + topicId);
        }
        topic.removeSubscriber(subscriberName);
        Set<String> subscribedTopics = subscriberTopics.get(subscriberName);
        if (subscribedTopics != null) {
            subscribedTopics.remove(topicId);
            if (subscribedTopics.isEmpty()) {
                subscriberTopics.remove(subscriberName);
            }
        }
    }

    @Override
    public List<String> getSubscriberCount(String topicId, String publisherName) throws RemoteException {
        List<String> result = new ArrayList<>();
        Topic topic = topics.get(topicId);
        if (topic == null) {
            throw new RemoteException("Topic not found: " + topicId);
        }
        if (!topic.getPublisherName().equals(publisherName)) {
            throw new RemoteException("Unauthorized: Publisher " + publisherName + " cannot access topic " + topicId);
        }
        result.add(topic.getTopicId() + " " + topic.getTopicName() + " " + topic.getSubscriberCount());
        return result;
    }

    @Override
    public synchronized void deleteTopic(String topicId, String publisherName) throws RemoteException {
        Topic topic = topics.get(topicId);
        if (topic == null) {
            throw new RemoteException("Topic not found: " + topicId);
        }
        if (!topic.getPublisherName().equals(publisherName)) {
            throw new RemoteException("Unauthorized: Publisher " + publisherName + " cannot delete topic " + topicId);
        }
        topics.remove(topicId);
        publisherTopics.get(publisherName).remove(topicId);
        // Remove this topic from all subscribers
        for (Set<String> subscribedTopics : subscriberTopics.values()) {
            subscribedTopics.remove(topicId);
        }
        // Remove empty subscriber entries
        subscriberTopics.entrySet().removeIf(entry -> entry.getValue().isEmpty());
    }

    @Override
    public List<String> getCurrentSubscriptions(String subscriberName) throws RemoteException {
        Set<String> subscribedTopics = subscriberTopics.get(subscriberName);
        if (subscribedTopics == null || subscribedTopics.isEmpty()) {
            return Collections.emptyList();
        }
        return subscribedTopics.stream()
                .map(topicId -> {
                    Topic topic = topics.get(topicId);
                    return topic.getTopicId() + " " + topic.getTopicName() + " " + topic.getPublisherName();
                })
                .collect(Collectors.toList());
    }
}