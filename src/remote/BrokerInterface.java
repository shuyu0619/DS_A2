package remote;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface BrokerInterface extends Remote {
    void createTopic(String topicId, String topicName, String publisherName) throws RemoteException;
    void publishMessage(String topicId, String message, String publisherName) throws RemoteException;
    List<String> listTopics() throws RemoteException;
    void subscribe(String topicId, String subscriberName, SubscriberCallbackInterface subscriber) throws RemoteException;
    void unsubscribe(String topicId, String subscriberName) throws RemoteException;
    List<String> getSubscriberCount(String topicId, String publisherName) throws RemoteException;
    void deleteTopic(String topicId, String publisherName) throws RemoteException;
    List<String> getCurrentSubscriptions(String subscriberName) throws RemoteException;
}