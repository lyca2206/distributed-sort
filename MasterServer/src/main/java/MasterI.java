import AppInterface.GroupingTask;
import AppInterface.Task;
import AppInterface.WorkerPrx;
import com.zeroc.Ice.Current;

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

    public Map<String, List<String>> groups;

    long startTime;
    long endTime;

    public MasterI(Queue<Task> queue, Map<String, WorkerPrx> workers, Map<String, Task> currentTasks, Map<String, List<String>> groups) {
        this.queue = queue;
        this.workers = workers;
        this.currentTasks = currentTasks;
        this.groups = groups;
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

            startTime = System.nanoTime();

            createGroupingTasks(fileName);
            System.out.println("Line 49: "+queue.toString());
            launchWorkers();
        }
    }

    private void createGroupingTasks(String fileName) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            long fileSize = getFileSize(fileName);
            long listSize = getLineCount(fileName);
            long taskAmount = fileSize * (4 * 2) / L2_CACHE + 2;
            long taskSize = listSize / taskAmount;
            int characters = (int) (Math.log(taskAmount) / Math.log(26 * 2 + 10)) + 2;

            System.out.println("Line 62: "+characters);

            for (long i = 0; i < taskAmount; i++) {
                String[] data = readData(br, taskSize);
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

    private String[] readData(BufferedReader br, long taskSize) throws IOException {
        String[] data = new String[(int) taskSize];

        for (long i = 0; i < taskSize; i++) {
            String line = br.readLine();

            if (line == null) {
                return data;
            }

            data[(int) i] = line;
        }

        return data;
    }

    private void launchWorkers() {
        workers.values().forEach(WorkerPrx::launch);
    }

    @Override
    public Task getTask(String id, Current current) {
        Task task = queue.poll();
        if (task != null) {
            currentTasks.put(id, task);
        }
        return task;
    }

    @Override
    public void addPartialResults(String[] array, Current current) {
        //TODO.
    }

    @Override
    public void addGroupingResults(Map<String, List<String>> groups, Current current) {
        Map<String, List<String>> localGroups = this.groups;

        groups.keySet().forEach((key) -> {
            if (!localGroups.containsKey(key)) { localGroups.put(key, groups.get(key)); }
            else { localGroups.get(key).addAll(groups.get(key)); }
        });

        if (queue.isEmpty()) {
            endTime = System.nanoTime();
            System.out.println("Line 132: "+(endTime - startTime) / 1000000);
            localGroups.keySet().forEach((key) -> {System.out.println(key+": "+localGroups.get(key).size());});
        }
    }

    private void shutdownWorkers() {
        workers.values().forEach(WorkerPrx::shutdown);
    }
}