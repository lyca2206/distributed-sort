import AppInterface.MasterPrx;
import AppInterface.WorkerPrx;
import com.zeroc.Ice.Communicator;
import com.zeroc.Ice.ObjectAdapter;
import com.zeroc.Ice.Properties;
import com.zeroc.Ice.Util;

import java.net.UnknownHostException;
import java.util.concurrent.LinkedBlockingQueue;
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

            masterPrx.signUp(communicator.getProperties().getProperty("workerHost"), workerPrx);
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

        Properties properties = communicator.getProperties();
        int corePoolSize = Integer.parseInt(properties.getProperty("corePoolSize"));
        int maximumPoolSize = Integer.parseInt(properties.getProperty("maximumPoolSize"));
        long keepAliveTime = Long.parseLong(properties.getProperty("keepAliveTime"));
        String masterHost = properties.getProperty("masterHost");
        String masterTemporalPath = properties.getProperty("masterTemporalPath");
        String workerHost = properties.getProperty("workerHost");
        String username = properties.getProperty("username").replace("\"","");
        String password = properties.getProperty("password").replace("\"","");;

        WorkerI worker = new WorkerI(corePoolSize, maximumPoolSize,
                keepAliveTime, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
                masterPrx, masterHost, masterTemporalPath, workerHost,
                username, password);
        adapter.add(worker, Util.stringToIdentity("Worker"));

        adapter.activate();

        return WorkerPrx.checkedCast(
                adapter.createProxy(Util.stringToIdentity("Worker")).ice_secure(false));
    }
}