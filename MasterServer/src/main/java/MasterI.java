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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public class MasterI implements AppInterface.Master {
    private static final long L2_CACHE = 4194304;

    private final Queue<Task> queue;
    private final Map<String, WorkerPrx> workers;
    private final Map<String, Map<String, Task>> currentTasks;
    private final Map<String, List<String>> groups;

    private boolean isProcessing;
    private long addingToResults;

    public MasterI(Queue<Task> queue, Map<String, WorkerPrx> workers,
                   Map<String, Map<String, Task>> currentTasks, Map<String, List<String>> groups) {
        this.queue = queue;
        this.workers = workers;
        this.currentTasks = currentTasks;
        this.groups = groups;
        isProcessing = false;
        addingToResults = 0;
    }

    @Override
    public void signUp(String workerId, WorkerPrx worker, Current current) {
        workers.put(workerId, worker);
        currentTasks.put(workerId, new ConcurrentHashMap<>());
    }

    public void initialize() throws IOException {
        try(BufferedReader br = new BufferedReader(new InputStreamReader(System.in)))
        {
            System.out.println("Enter the name of the file to be sorted. Be aware that you need to deploy the Workers first.");
            String fileName = "./" + br.readLine();

            System.out.println("Sorting has started.");
            startProcessing(fileName);
        }
    }

    private void startProcessing(String fileName) throws IOException {
        long startTime = System.nanoTime();
        isProcessing = true;
        launchWorkers();
        new Thread(this::startPingingWorkers).start();

        createGroupingTasks(fileName);
        doNextStepAfterFinalization();

        createSortingTasks();
        doNextStepAfterFinalization();
        isProcessing = false;

        shutdownWorkers();
        processAndServeResult();
        long endTime = System.nanoTime();

        AtomicLong listSize = new AtomicLong();
        groups.values().forEach((list) -> {
            listSize.addAndGet(list.size());
        });

        System.out.println(listSize);
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
                catch (TimeoutException e) {
                    resetTasks(key);}
            });
            try { Thread.sleep(5000); }
            catch (InterruptedException e) { throw new RuntimeException(e); }
        }
    }

    private void resetTasks(String key) {
        Map<String, Task> map = currentTasks.remove(key);
        queue.addAll(map.values());
        workers.remove(key);
    }

    private void createGroupingTasks(String fileName) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            long fileSize = getFileSize(fileName);
            long listSize = getLineCount(fileName);

            System.out.println(listSize);

            long taskAmount = fileSize * (4 * 2) / L2_CACHE + 1;
            long taskSize = listSize / taskAmount + 1;
            int characters = (int) (Math.log(taskAmount) / Math.log(26 * 2 + 10)) + 1;

            for (long i = 0; i < taskAmount; i++) {
                ArrayList<String> data = readData(br, taskSize);
                Task task = new GroupingTask(String.valueOf(i), data, new HashMap<>(), characters);
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
            AtomicLong totalSize = new AtomicLong();
            currentTasks.forEach(((string, stringTaskMap) -> totalSize.addAndGet(stringTaskMap.size())));
            if (queue.isEmpty() && totalSize.get() <= 0 && addingToResults <= 0) {
                break;
            }
        }
    }

    private void createSortingTasks() {
        //TODO.
    }

    private void processAndServeResult() {
        //TODO.
    }

    @Override
    public Task getTask(String workerId, Current current) {
        Task task = queue.poll();
        if (task != null) { currentTasks.get(workerId).put(task.id, task); }
        return task;
    }

    @Override
    public void addGroupingResults(String workerId, String taskId, Map<String, List<String>> groups, Current current) {
        currentTasks.get(workerId).remove(taskId);
        addingToResults++;
        Map<String, List<String>> localGroups = this.groups;
        groups.keySet().forEach((key) -> {
            if (!localGroups.containsKey(key)) { localGroups.put(key, groups.get(key)); }
            else { localGroups.get(key).addAll(groups.get(key)); }
        });
        addingToResults--;
    }

    @Override
    public void addSortingResults(String workerId, String taskId, List<String> array, Current current) {
        currentTasks.get(workerId).remove(taskId);
        addingToResults++;
        //TODO.
        addingToResults--;
    }

    private void shutdownWorkers() {
        workers.values().forEach(WorkerPrx::shutdown);
    }
}