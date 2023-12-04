import com.zeroc.Ice.Communicator;
import com.zeroc.Ice.ObjectAdapter;
import com.zeroc.Ice.Properties;
import com.zeroc.Ice.Util;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;


public class MasterServer {
    public static void main(String[] args) {
        try {
            serverInit(args);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void serverInit(String[] args) throws IOException, InterruptedException {
        try(Communicator communicator = Util.initialize(args, "master.cfg"))
        {
            serviceInit(communicator);
            System.out.println("Sorting has started.");

            communicator.waitForShutdown();
        }
    }

    private static void serviceInit(Communicator communicator) throws IOException {
        ObjectAdapter adapter = communicator.createObjectAdapter("MasterServer");

        Properties properties = communicator.getProperties();

        int maxThreads = Integer.parseInt(properties.getProperty("MaxThreads"));
        int linesPerTask = Integer.parseInt(properties.getProperty("LinesPerTask"));

        MasterI master = new MasterI(
                Collections.synchronizedList(new LinkedList<>()),communicator,maxThreads, linesPerTask);
        adapter.add(master, Util.stringToIdentity("Master"));
        adapter.activate();
        System.out.println("Master has been started.");

        master.initialize();
    }
}