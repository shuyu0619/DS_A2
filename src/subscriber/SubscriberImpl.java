package subscriber;

import remote.SubscriberCallbackInterface;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * SubscriberImpl implements SubscriberCallbackInterface.
 * It defines how subscribers handle notifications from the broker.
 */
public class SubscriberImpl extends UnicastRemoteObject implements SubscriberCallbackInterface {
    private static final Logger logger = Logger.getLogger(SubscriberImpl.class.getName());

    private final String subscriberName;

    public SubscriberImpl(String subscriberName) throws RemoteException {
        super();
        this.subscriberName = subscriberName;
        logger.info("SubscriberImpl created for " + subscriberName);
    }

    @Override
    public void notifySubscriber(String topicId, String topicName, String publisherName, String message) throws RemoteException {
        String timestamp = new SimpleDateFormat("dd/MM HH:mm:ss").format(new Date());
        String formattedMessage = "[" + timestamp + "] [" + topicId + ":" + topicName + "] [" + message + "]";
        System.out.println(formattedMessage);
        logger.info("Received message: " + formattedMessage);
    }
}
