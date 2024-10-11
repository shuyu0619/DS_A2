package directory;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class DirectoryServer {
    public static void main(String[] args) {
        try {
            DirectoryServiceImpl directoryService = new DirectoryServiceImpl();
            Registry registry = LocateRegistry.createRegistry(1099);
            registry.rebind("DirectoryService", directoryService);
            System.out.println("Directory Service started on port 1099");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
