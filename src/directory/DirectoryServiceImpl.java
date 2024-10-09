package directory;

import remote.DirectoryServiceInterface;
import remote.BrokerInfo;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.ArrayList;

public class DirectoryServiceImpl extends UnicastRemoteObject implements DirectoryServiceInterface {
    private final ConcurrentHashMap<String, BrokerInfo> brokers = new ConcurrentHashMap<>();

    public DirectoryServiceImpl() throws RemoteException {
        super();
    }

    @Override
    public synchronized List<BrokerInfo> registerAndGetBrokerList(String brokerId, String ip, int port) throws RemoteException {
        brokers.put(brokerId, new BrokerInfo(brokerId, ip, port));
        return new ArrayList<>(brokers.values());
    }

    @Override
    public synchronized List<BrokerInfo> getBrokerList() throws RemoteException {
        return new ArrayList<>(brokers.values());
    }
}
