import AppInterface.MasterPrx;
import AppInterface.MergeTask;
import AppInterface.StringSortTask;
import AppInterface.Task;
import com.zeroc.Ice.Communicator;
import com.zeroc.Ice.Current;
import sorter.MSDRadixSortTask;
import merger.MergeArraysTask;
import java.util.concurrent.ForkJoinPool;

public class WorkerI implements AppInterface.Worker {
    private final MasterPrx masterPrx;
    private final Communicator communicator;
    private volatile boolean isShutdown;

    private ForkJoinPool pool;

    private int workerId;

    public WorkerI(MasterPrx masterPrx, Communicator communicator) {
        this.masterPrx = masterPrx;
        isShutdown = false;
        this.communicator = communicator;
    }

    @Override
    public void launch(Current current) {
        pool = new ForkJoinPool(4);
        startWorker();
    }

    private void startWorker(){
        new Thread(() -> {
            System.out.println("Worker launched");
            while (!isShutdown) {
                System.out.println("Task requested");
                try {
                    requestTask();
                } catch (InterruptedException ignored) {}
            }
            shutdown();
        }).start();
    }

    private void requestTask() throws InterruptedException {

        long t1 = System.currentTimeMillis();
        Task task = masterPrx.getTask(workerId);
        long t2 = System.currentTimeMillis();
        System.out.printf("Task arrived (%d ms)\n",t2-t1);

        String[] response;

        if(task instanceof StringSortTask){
            System.out.println("Started StringSortTask processing!");
            t1 = System.currentTimeMillis();
            response = processSortTask((StringSortTask) task);
            t2 = System.currentTimeMillis();
            System.out.printf("Finished StringSortTask processing! (%d ms)\n",t2-t1);
        }
        else if (task instanceof MergeTask){
            System.out.println("Started MergeTask processing!");
            t1 = System.currentTimeMillis();
            response = processMergeTask((MergeTask) task);
            t2 = System.currentTimeMillis();
            System.out.printf("Finished MergeTask processing! (%d ms)\n",t2-t1);
        }
        else if (task == null) {
            Thread.sleep(1500);
            return;
        }
        else{
            throw  new UnsupportedOperationException("The provided task is not supported!");
        }
        System.out.println("Sent response!");
        t1 = System.currentTimeMillis();
        masterPrx. addPartialResults(task.taskId, workerId,response);
        t2 = System.currentTimeMillis();
        System.out.printf("Time to sent! (%d ms)\n",t2-t1);
    }

    private String[] processSortTask(StringSortTask sortTask){
        String[] array = sortTask.array;

        MSDRadixSortTask task = new MSDRadixSortTask(sortTask.array);
        pool.invoke(task);

        return array;
    }

    private String[] processMergeTask(MergeTask mergeTask){
        String[] array1 = mergeTask.array1;
        String[] array2 = mergeTask.array2;

        MergeArraysTask task = new MergeArraysTask(array1, array2);

        return pool.invoke(task);
    }


    @Override
    public void shutdown(Current current) {
        isShutdown = true;
    }

    @Override
    public int ping(Current current) {
        return workerId;
    }

    private void shutdown(){
        System.out.println("Shutting down");
        pool.shutdown();
        communicator.shutdown();
    }

    public void setId(int workerId) {
        this.workerId = workerId;
    }
}
