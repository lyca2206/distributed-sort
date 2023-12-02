import AppInterface.MasterPrx;
import AppInterface.WorkerPrx;
import com.zeroc.Ice.Communicator;
import com.zeroc.Ice.ObjectAdapter;
import com.zeroc.Ice.Util;

import java.net.UnknownHostException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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

            masterPrx.signUp(workerPrx);
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

        WorkerI worker = new WorkerI(0, 16, 32, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), masterPrx);
        adapter.add(worker, Util.stringToIdentity("Worker"));

        adapter.activate();

        return WorkerPrx.checkedCast(
                adapter.createProxy(Util.stringToIdentity("Worker")).ice_secure(false));
    }
}