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
    private static final long L3_CACHE = 16777216;

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

            startTime = System.currentTimeMillis();

            createTasks(fileName);
            launchWorkers();
        }
    }

    private void createTasks(String fileName) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            long fileSize = getFileSize(fileName);
            long listSize = getLineCount(fileName);
            long taskAmount = fileSize * 8 / L2_CACHE;
            long taskSize = listSize / taskAmount;
            int characters = (int) (Math.log(taskAmount) / Math.log(26 * 2 + 10));

            for (long i = 0; i < taskAmount; i++) {
                String[] data = readData(br, taskSize);
                Task task = createGroupingTask(data, characters);
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

    private Task createGroupingTask(String[] data, int characters) {
        return new GroupingTask(data, new HashMap<>(), characters) {
            @Override
            public void run() {
                for (String string : data) {
                    String key = string.substring(0, characters);
                    if (groups.containsKey(key)) { groups.get(key).add(string); }
                    else { groups.put(key, new ArrayList<>()); }
                }
            }
        };
    }

    private Task createSortingTask(String[] data) {
        return new Task(data) {
            @Override
            public void run() {
                Arrays.sort(data);
            }
        };
    }

    private void launchWorkers() {
        workers.values().forEach(WorkerPrx::launch);
    }

    @Override
    public Task getTask(String id, Current current) {
        Task task = queue.poll();
        currentTasks.put(id, task);
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
            endTime = System.currentTimeMillis();
            System.out.println((endTime - startTime) / 1000);
        }
    }

    private void shutdownWorkers() {
        workers.values().forEach(WorkerPrx::shutdown);
    }
}