import com.zeroc.Ice.Communicator;
import com.zeroc.Ice.ObjectAdapter;
import com.zeroc.Ice.Util;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;

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
            System.out.println("Sorting has started.");

            communicator.waitForShutdown();
        }
    }

    private static void serviceInit(Communicator communicator) throws IOException {
        ObjectAdapter adapter = communicator.createObjectAdapter("MasterServer");

        MasterI master = new MasterI(
                Collections.synchronizedList(new LinkedList<>())
        );
        adapter.add(master, Util.stringToIdentity("Master"));
        adapter.activate();
        System.out.println("Master has been started.");

        master.initialize();
    }
}