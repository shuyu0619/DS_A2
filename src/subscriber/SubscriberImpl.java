package subscriber;

import remote.SubscriberCallbackInterface;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.text.SimpleDateFormat;
import java.util.Date;

public class SubscriberImpl extends UnicastRemoteObject implements SubscriberCallbackInterface {

    public SubscriberImpl() throws RemoteException {
        super();
    }

    @Override
    public void notifySubscriber(String topicId, String topicName, String publisherName, String message) throws RemoteException {
        String timestamp = new SimpleDateFormat("dd/MM HH:mm:ss").format(new Date());
        String formattedMessage = "[" + timestamp + "] [" + topicId + ":" + topicName + ":] [" + message + "]";
        System.out.println(formattedMessage);
    }
}
