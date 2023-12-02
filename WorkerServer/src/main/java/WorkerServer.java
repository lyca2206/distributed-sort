import AppInterface.MasterPrx;
import AppInterface.WorkerPrx;
import com.zeroc.Ice.Communicator;
import com.zeroc.Ice.ObjectAdapter;
import com.zeroc.Ice.Util;

import java.net.UnknownHostException;

public class WorkerServer {
    public static void main(String[] args) {
        try {
            serverInit(args);
        } catch (InterruptedException | UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    private static void serverInit(String[] args) throws InterruptedException, UnknownHostException {
        try(Communicator communicator = Util.initialize(args, "worker.cfg"))
        {
            MasterPrx masterPrx = createMasterProxy(communicator);
            WorkerPrx workerPrx = createWorkerProxy(communicator, masterPrx);

            masterPrx.signUp(communicator.getProperties().getProperty("ID"), workerPrx);
            System.out.println("Worker has been started.");

            communicator.waitForShutdown();
        }
    }

    private static MasterPrx createMasterProxy(Communicator communicator) {
        MasterPrx masterPrx = MasterPrx.checkedCast(
                communicator.propertyToProxy("MasterServer.Proxy")).ice_secure(false);

        if (masterPrx == null)
            throw new Error("Invalid Proxy: Property might not exist in the configuration file.");

        return masterPrx;
    }

    private static WorkerPrx createWorkerProxy(Communicator communicator, MasterPrx masterPrx) {
        ObjectAdapter adapter = communicator.createObjectAdapter("WorkerServer");

        WorkerI worker = new WorkerI(masterPrx,
                communicator.getProperties().getProperty("WorkerServer.ID"));
        adapter.add(worker, Util.stringToIdentity("Worker"));

        adapter.activate();

        return WorkerPrx.checkedCast(
                adapter.createProxy(Util.stringToIdentity("Worker")).ice_secure(false));
    }
}