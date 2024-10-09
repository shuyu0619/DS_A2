package remote;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface DirectoryServiceInterface extends Remote {
    List<BrokerInfo> registerAndGetBrokerList(String brokerId, String ip, int port) throws RemoteException;
    List<BrokerInfo> getBrokerList() throws RemoteException;
}
