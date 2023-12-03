import AppInterface.GroupingTask;
import AppInterface.Task;
import AppInterface.WorkerPrx;
import com.zeroc.Ice.Current;
import com.zeroc.Ice.TimeoutException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

public class MasterI implements AppInterface.Master {
    private static final long L2_CACHE = 4194304;

    private final Queue<Task> queue;
    private final Map<String, WorkerPrx> workers;
    private final Map<String, Task> currentTasks;
    private final Map<String, List<String>> groups;

    private boolean isProcessing;
    private long addingToResults;

    public MasterI(Queue<Task> queue, Map<String, WorkerPrx> workers, Map<String,
            Task> currentTasks, Map<String, List<String>> groups) {
        this.queue = queue;
        this.workers = workers;
        this.currentTasks = currentTasks;
        this.groups = groups;
        isProcessing = false;
        addingToResults = 0;
    }

    @Override
    public void signUp(String id, WorkerPrx worker, Current current) {
        workers.put(id, worker);
    }

    public void initialize() throws IOException {
        try(BufferedReader br = new BufferedReader(new InputStreamReader(System.in)))
        {
            System.out.println("Enter the name of the file to be sorted. Be aware that you need to deploy the Workers first.");
            String fileName = "./" + br.readLine();

            startProcessing(fileName);
        }
    }

    private void startProcessing(String fileName) throws IOException {
        long startTime = System.nanoTime();

        isProcessing = true;
        launchWorkers();
        Thread thread = new Thread(this::startPingingWorkers);
        thread.start();

        createGroupingTasks(fileName);
        doNextStepAfterFinalization();

        createSortingTasks();
        doNextStepAfterFinalization();
        isProcessing = false;

        shutdownWorkers();
        processAndServeResult();

        long endTime = System.nanoTime();
        System.out.println("Time: " + (endTime - startTime) + " ns.");
    }

    private void launchWorkers() {
        workers.values().forEach(WorkerPrx::launch);
    }

    private void startPingingWorkers() {
        while (isProcessing) {
            workers.keySet().forEach((key) ->
            {
                WorkerPrx worker = workers.get(key);
                try { worker.ping(); }
                catch (TimeoutException e) {resetTask(key);}
            });
            try { Thread.sleep(5000); }
            catch (InterruptedException e) { throw new RuntimeException(e); }
        }
    }

    private void resetTask(String key) {
        Task task = currentTasks.remove(key);
        if (task != null) { queue.add(task); }
    }

    private void createGroupingTasks(String fileName) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            long fileSize = getFileSize(fileName);
            long listSize = getLineCount(fileName);
            long taskAmount = fileSize * (4 * 2) / L2_CACHE + 1;
            long taskSize = listSize / taskAmount;
            int characters = (int) (Math.log(taskAmount) / Math.log(26 * 2 + 10)) + 1;

            for (long i = 0; i < taskAmount; i++) {
                ArrayList<String> data = readData(br, taskSize);
                Task task = new GroupingTask(data, new HashMap<>(), characters);
                queue.add(task);
            }
        }
    }

    private long getFileSize(String fileName) throws IOException {
        return Files.size(Paths.get(fileName));
    }

    private long getLineCount(String fileName) throws IOException {
        try (Stream<String> fileStream = Files.lines(Paths.get(fileName))) {
            return fileStream.count();
        }
    }

    private ArrayList<String> readData(BufferedReader br, long taskSize) throws IOException {
        ArrayList<String> data = new ArrayList<>((int) taskSize);
        for (long i = 0; i < taskSize; i++) {
            String line = br.readLine();
            if (line == null) { return data; }
            data.add(line);
        }
        return data;
    }

    private void doNextStepAfterFinalization() {
        while (true) {
            if (queue.isEmpty() && currentTasks.isEmpty() && addingToResults == 0) {
                break;
            }
        }
    }

    private void createSortingTasks() {
        groups.values().forEach((array) -> {
        });
    }

    private void processAndServeResult() {
        //TODO.
    }

    @Override
    public Task getTask(String id, Current current) {
        Task task = queue.poll();
        if (task != null) { currentTasks.put(id, task); }
        else { currentTasks.remove(id); }
        return task;
    }

    @Override
    public void addGroupingResults(Map<String, List<String>> groups, Current current) {
        addingToResults++;
        Map<String, List<String>> localGroups = this.groups;
        groups.keySet().forEach((key) -> {
            if (!localGroups.containsKey(key)) { localGroups.put(key, groups.get(key)); }
            else { localGroups.get(key).addAll(groups.get(key)); }
        });
        addingToResults--;
    }

    @Override
    public void addSortingResults(List<String> array, Current current) {
        addingToResults++;
        //TODO.
        addingToResults--;
    }

    private void shutdownWorkers() {
        workers.values().forEach(WorkerPrx::shutdown);
    }
}