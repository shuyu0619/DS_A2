package subscriber;

import remote.SubscriberCallbackInterface;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.text.SimpleDateFormat;
import java.util.Date;

public class SubscriberImpl extends UnicastRemoteObject implements SubscriberCallbackInterface {
    private String subscriberName;

    public SubscriberImpl(String subscriberName) throws RemoteException {
        super();
        this.subscriberName = subscriberName;
    }

    @Override
    public void notifySubscriber(String topicId, String message) throws RemoteException {
        String timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        System.out.println("[" + timestamp + "] [" + subscriberName + "] Message from topic [" + topicId + "]: " + message);
    }
}
