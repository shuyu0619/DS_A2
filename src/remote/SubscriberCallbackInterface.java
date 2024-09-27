package remote;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Defines the remote method for Subscribers to receive notifications from the Broker.
 */
public interface SubscriberCallbackInterface extends Remote {
    void notifySubscriber(String topicId, String topicName, String publisherName, String message) throws RemoteException;
}
