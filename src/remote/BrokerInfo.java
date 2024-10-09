package remote;

import java.io.Serializable;

public class BrokerInfo implements Serializable {
    private String brokerId;
    private String ip;
    private int port;

    public BrokerInfo(String brokerId, String ip, int port) {
        this.brokerId = brokerId;
        this.ip = ip;
        this.port = port;
    }

    public String getBrokerId() {
        return brokerId;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }
}
