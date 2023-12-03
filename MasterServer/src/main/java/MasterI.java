import AppInterface.Task;
import AppInterface.WorkerPrx;
import com.zeroc.Ice.Current;
import com.zeroc.Ice.Value;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

public class MasterI implements AppInterface.Master {
    private static final long CACHE_SIZE = 16777216;

    private final Queue<Task> queue;
    private final Map<String, WorkerPrx> workers;
    private final Map<String, Task> currentTasks;

    private final Map<String, List<String>> groups;

    public MasterI(Queue<Task> queue, Map<String, WorkerPrx> workers, Map<String, Task> currentTasks, Map<String, List<String>> groups) {
        this.queue = queue;
        this.workers = workers;
        this.currentTasks = currentTasks;
        this.groups = groups;
    }


    long startTime;
    long endTime;


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
            System.out.println(queue.toString());
            launchWorkers();
        }
    }

    private void createTasks(String fileName) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            long fileSize = getFileSize(fileName);
            long listSize = getLineCount(fileName);
            long taskAmount = fileSize * 32 / CACHE_SIZE;
            long taskSize = listSize / taskAmount;

            for (long i = 0; i < taskAmount; i++) {
                String[] data = readData(br, taskSize);
                Task task = createGroupingTask(data);
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

    private Task createGroupingTask(String[] data) {
        return new Task(null, data) {
            @Override
            public void run() {
                Map<String, ArrayList<String>> map = new HashMap<>();
                for (String s : data) {
                    String key = s.substring(0, 2);
                    if (map.containsKey(key)) { map.get(key).add(s); }
                    else { map.put(key, new ArrayList<>()); }
                }
                for (ArrayList<String> list : map.values()) {
                    this.worker.addGroupResults(list.toArray(new String[0]));
                }
            }
        };
    }

    private Task createSortingTask(String[] data) {
        return new Task(null, data) {
            @Override
            public void run() {
                Arrays.sort(data);
                worker.addSortResults(data);
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
    public void addGroupResults(String[] array, Current current) {
        String key = array[0].substring(0, 2);
        if (groups.containsKey(key)) {
            List<String> list = groups.get(key);
            list.addAll(Arrays.asList(array));
        } else {
            groups.put(key, Arrays.asList(array));
        }

        if (queue.isEmpty()) {
            endTime = System.currentTimeMillis();
            System.out.println((endTime - startTime) / 1000);
        }
    }

    @Override
    public void addSortResults(String[] array, Current current) {
    }

    private void shutdownWorkers() {
        workers.values().forEach(WorkerPrx::shutdown);
    }
}