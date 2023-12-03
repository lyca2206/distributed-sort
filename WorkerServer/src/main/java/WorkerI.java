import AppInterface.MasterPrx;
import AppInterface.MergeTask;
import AppInterface.StringSortTask;
import AppInterface.Task;
import com.zeroc.Ice.Current;
import sorter.MSDRadixSortTask;
import merger.MergeArraysTask;

import java.util.Arrays;
import java.util.concurrent.ForkJoinPool;

public class WorkerI implements AppInterface.Worker {
    private final MasterPrx masterPrx;
    private volatile boolean isShutdown;

    private ForkJoinPool pool;

    public WorkerI(MasterPrx masterPrx) {
        this.masterPrx = masterPrx;
        isShutdown = false;
    }

    @Override
    public void launch(Current current) {
        pool = new ForkJoinPool(15);
        startWorker();
    }

    private void startWorker(){
        new Thread(() -> {
            System.out.println("Worker launched");
            while (!isShutdown) {
                System.out.println("Task requested");
                requestTask();
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            shutdown();
        }).start();
    }

    private void requestTask(){
        Task task = masterPrx.getTask();



        if(task instanceof StringSortTask){
            System.out.println("Started StringSortTask processing!");
            processSortTask((StringSortTask) task);
            System.out.println("Finished StringSortTask processing!");
        }
        else if (task instanceof MergeTask){
            System.out.println("Started MergeTask processing!");
            processMergeTask((MergeTask) task);
            System.out.println("Finished MergeTask processing!");
        }
        else{
            throw  new UnsupportedOperationException("The provided task is not supported!");
        }
    }

    private void processSortTask(StringSortTask sortTask){
        String[] array = sortTask.array;

        System.out.println("BEFORE: " + Arrays.toString( array));

        MSDRadixSortTask task = new MSDRadixSortTask(sortTask.array);
        pool.invoke(task);

        System.out.println("AFTER: " + Arrays.toString( array));

        masterPrx.addPartialResults(array);
    }

    private void processMergeTask(MergeTask mergeTask){
        String[] array1 = mergeTask.array1;
        String[] array2 = mergeTask.array2;

        MergeArraysTask task = new MergeArraysTask(array1, array2);
        String[] mergedArray = pool.invoke(task);

        masterPrx.addPartialResults(mergedArray);
    }


    @Override
    public void shutdown(Current current) {
        isShutdown = true;
    }

    private void shutdown(){
        pool.shutdown();
        //Shutdown server (terminate server)???
    }

}
