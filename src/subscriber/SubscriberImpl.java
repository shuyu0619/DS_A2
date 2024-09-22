package subscriber;

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
    public void notifySubscriber(String message) throws RemoteException {
        try {
            String timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
            System.out.println("[" + timestamp + "] [" + subscriberName + "] Received message: " + message);
        } catch (Exception e) {
            System.err.println("Error formatting date: " + e.getMessage());
            // Still print the message even if date formatting fails
            System.out.println("[" + subscriberName + "] Received message: " + message);
        }
    }
}
