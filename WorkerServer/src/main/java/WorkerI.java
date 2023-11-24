import AppInterface.MasterPrx;
import com.zeroc.Ice.Current;

public class WorkerI implements AppInterface.Worker{
    private final MasterPrx masterPrx;

    public WorkerI(MasterPrx masterPrx) {
        this.masterPrx = masterPrx;
    }

    @Override
    public void launch(Current current) {
    }

    @Override
    public void shutdown(Current current) {
    }
}