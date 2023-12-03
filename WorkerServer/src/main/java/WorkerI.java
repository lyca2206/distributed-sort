import AppInterface.GroupingTask;
import AppInterface.MasterPrx;
import AppInterface.Task;
import com.zeroc.Ice.Current;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class WorkerI extends ThreadPoolExecutor implements AppInterface.Worker {
    private final MasterPrx masterPrx;
    private final String id;
    private boolean isRunning;

    public WorkerI(int corePoolSize, int maximumPoolSize, long keepAliveTime,
                   TimeUnit unit, BlockingQueue<Runnable> workQueue, MasterPrx masterPrx, String id) {
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

    @Override
    public void ping(Current current) {}

    private void startTaskPolling() {
        while (isRunning) {
            if (getActiveCount() < getMaximumPoolSize()) { getThenExecuteTask(); }
        }
    }

    private void getThenExecuteTask() {
        Task task = masterPrx.getTask(id);
        if (task != null) {
            if (task instanceof GroupingTask) {
                GroupingTask groupingTask = (GroupingTask) task;
                execute(() -> {
                    for (String string : groupingTask.data) {
                        String key = string.substring(0, groupingTask.characters);
                        if (!groupingTask.groups.containsKey(key)) { groupingTask.groups.put(key, new ArrayList<>()); }
                        groupingTask.groups.get(key).add(string);
                    }
                    masterPrx.addGroupingResults(id, String.valueOf(groupingTask.id), groupingTask.groups);
                });
            } else {
                execute(() -> {
                    task.data.sort(Comparator.naturalOrder());
                    masterPrx.addSortingResults(id, String.valueOf(task.id), task.data);
                });
            }
        }
    }

    @Override
    public void shutdown(Current current) {
        isRunning = false;
    }
}