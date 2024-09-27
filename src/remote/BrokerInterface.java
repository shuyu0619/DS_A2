package remote;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

/**
 * Defines methods for communication between publishers/subscribers and brokers.
 */
public interface BrokerInterface extends Remote {
    // Publisher methods
    String createTopic(String topicId, String topicName, String publisherName) throws RemoteException;
    String publishMessage(String topicId, String message, String publisherName) throws RemoteException;
    List<String> getSubscriberCount(String topicId, String publisherName) throws RemoteException;
    String deleteTopic(String topicId, String publisherName) throws RemoteException;
    String handlePublisherCrash(String publisherName) throws RemoteException;

    // Subscriber methods
    List<String> listTopics() throws RemoteException;
    String subscribe(String topicId, String subscriberName, SubscriberCallbackInterface subscriber) throws RemoteException;
    String unsubscribe(String topicId, String subscriberName) throws RemoteException;
    List<String> getCurrentSubscriptions(String subscriberName) throws RemoteException;

    // Broker synchronization methods
    void synchronizeTopic(String topicId, String topicName, String publisherName) throws RemoteException;
    void synchronizeTopicDeletion(String topicId) throws RemoteException;
    void synchronizeSubscription(String topicId, String subscriberName, String action, String brokerId) throws RemoteException;
    void forwardMessage(String topicId, String message, String publisherName) throws RemoteException;

    // Broker identification methods
    String getBrokerIdentifier() throws RemoteException;
    int getLocalSubscriberCount(String topicId) throws RemoteException;
    List<String> getKnownBrokers() throws RemoteException;
}
