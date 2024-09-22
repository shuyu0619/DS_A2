package broker;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import subscriber.SubscriberCallbackInterface;

public interface BrokerInterface extends Remote {

    // Publisher methods
    void createTopic(String topicId, String topicName, String publisherName) throws RemoteException;
    void publishMessage(String topicId, String message, String publisherName) throws RemoteException;

    // Subscriber methods
    List<String> listTopics() throws RemoteException;
    void subscribe(String topicId, String subscriberName, SubscriberCallbackInterface subscriber) throws RemoteException;
}


