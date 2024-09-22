package remote;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface SubscriberCallbackInterface extends Remote {
    void notifySubscriber(String topicId, String message) throws RemoteException;
}
