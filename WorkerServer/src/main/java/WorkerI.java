import AppInterface.MasterPrx;
import AppInterface.Task;
import com.zeroc.Ice.Current;

public class WorkerI implements AppInterface.Worker {
    private final MasterPrx masterPrx;
    private final String id;
    private boolean isRunning;

    public WorkerI(MasterPrx masterPrx, String id) {
        this.masterPrx = masterPrx;
        this.id = id;
        isRunning = false;
    }

    @Override
    public void launch(Current current) {
        isRunning = true;
        startTaskPolling();
    }

    private void startTaskPolling() {
        while (isRunning) {
            getThenExecuteTask();
        }
    }

    public void getThenExecuteTask() {
        Task task = (Task) masterPrx.getTask(id);
        if (task != null) {
            task.run();
            masterPrx.addPartialResults(task.data);
        }
    }

    @Override
    public void shutdown(Current current) {
        isRunning = false;
    }
}