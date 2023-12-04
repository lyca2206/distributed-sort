import com.zeroc.Ice.Communicator;
import com.zeroc.Ice.ObjectAdapter;
import com.zeroc.Ice.Properties;
import com.zeroc.Ice.Util;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;

public class MasterServer {
    public static void main(String[] args) {
        try {
            serverInit(args);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void serverInit(String[] args) throws IOException {
        try(Communicator communicator = Util.initialize(args, "master.cfg"))
        {
            serviceInit(communicator);
            communicator.waitForShutdown();
        }
    }

    private static void serviceInit(Communicator communicator) throws IOException {
        ObjectAdapter adapter = communicator.createObjectAdapter("MasterServer");

        Properties properties = communicator.getProperties();
        long pingMilis = Long.parseLong(properties.getProperty("pingMilis"));
        long batchSize = Long.parseLong(properties.getProperty("batchSize"));

        MasterI master = new MasterI(
                new ConcurrentLinkedQueue<>(), new ConcurrentHashMap<>(),
                new ConcurrentHashMap<>(), new ConcurrentSkipListMap<>(), pingMilis);
        adapter.add(master, Util.stringToIdentity("Master"));
        adapter.activate();
        System.out.println("Master has been started.");

        master.initialize(batchSize);
    }
}