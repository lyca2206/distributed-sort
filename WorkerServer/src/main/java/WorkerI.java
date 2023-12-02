import AppInterface.MasterPrx;
import AppInterface.Task;
import com.zeroc.Ice.Current;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class WorkerI extends ThreadPoolExecutor implements AppInterface.Worker {
    private final MasterPrx masterPrx;
    private boolean isRunning;

    public WorkerI(int corePoolSize, int maximumPoolSize,
                   long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, MasterPrx masterPrx) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
        this.masterPrx = masterPrx;
        isRunning = false;
    }

    @Override
    public void launch(Current current) {
        isRunning = true;
        startTaskPolling();
    }

    private void startTaskPolling() {
        while (isRunning) {
            if (getPoolSize() < getMaximumPoolSize()) { getThenExecuteTask(); }
        }
    }

    public void getThenExecuteTask() {
        Task task = (Task) masterPrx.getTask();
        if (task != null) {
            execute(task);
        }
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        Task task = (Task) r;
        masterPrx.addPartialResults(task.data);
    }

    @Override
    public void shutdown(Current current) {
        isRunning = false;
    }
}