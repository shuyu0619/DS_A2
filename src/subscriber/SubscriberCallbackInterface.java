package subscriber;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface SubscriberCallbackInterface extends Remote {
    public void notifySubscriber(String message) throws RemoteException;
}
