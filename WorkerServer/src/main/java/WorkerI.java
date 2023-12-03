import AppInterface.MasterPrx;
import AppInterface.Task;
import com.zeroc.Ice.Current;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class WorkerI extends ThreadPoolExecutor implements AppInterface.Worker {
    private final MasterPrx masterPrx;
    private final String id;
    private boolean isRunning;

    public WorkerI(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
                   BlockingQueue<Runnable> workQueue, MasterPrx masterPrx, String id) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
        this.masterPrx = masterPrx;
        this.id = id;
        isRunning = false;
    }

    @Override
    public void launch(Current current) {
        isRunning = true;
        Thread thread = new Thread(this::startTaskPolling);
        thread.start();
    }

    private void startTaskPolling() {
        while (isRunning) {
            if (getPoolSize() < getMaximumPoolSize()) { getThenExecuteTask(); }
        }
    }

    public void getThenExecuteTask() {
        Task task = masterPrx.getTask(id);
        if (task != null) {
            execute(task);
        }
    }

    @Override
    public void addGroupResults(String[] array, Current current) {
        masterPrx.addGroupResults(array);
    }

    @Override
    public void addSortResults(String[] array, Current current) {
        masterPrx.addSortResults(array);
    }

    @Override
    public void shutdown(Current current) {
        isRunning = false;
    }
}