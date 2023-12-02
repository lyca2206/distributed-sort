import AppInterface.MasterPrx;
import com.zeroc.Ice.Current;

public class WorkerI implements AppInterface.Worker {
    private final MasterPrx masterPrx;

    public WorkerI(MasterPrx masterPrx) {
        this.masterPrx = masterPrx;
    }

    @Override
    public void launch(Current current) {
        // Implement logic to initialize and start worker threads
        // ...
    }

    private void processSortTask(){

    }


    @Override
    public void shutdown(Current current) {
        // Implement logic to gracefully shut down worker threads
        // ...
        //Shutdown server (terminate server)
    }
}
