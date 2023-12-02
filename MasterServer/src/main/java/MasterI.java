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
import java.util.List;
import java.util.Queue;
import java.util.stream.Stream;

public class MasterI implements AppInterface.Master {
    private static final long CACHE_SIZE = 16777216;

    private final Queue<Task> queue;
    private final List<WorkerPrx> workers;

    public MasterI(Queue<Task> queue, List<WorkerPrx> workers) {
        this.queue = queue;
        this.workers = workers;
    }

    @Override
    public void signUp(WorkerPrx worker, Current current) {
        workers.add(worker);
    }

    public void initialize() throws IOException {
        try(BufferedReader br = new BufferedReader(new InputStreamReader(System.in)))
        {
            System.out.println("Enter the name of the file to be sorted. Be aware that you need to deploy the Workers first.");
            String fileName = "./" + br.readLine();

            createTasks(fileName);
            launchWorkers();
        }
    }

    private void createTasks(String fileName) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            long fileSize = getFileSize(fileName);
            long listSize = getLineCount(fileName);
            long taskAmount = fileSize / CACHE_SIZE;
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
        return new Task(data) {
            @Override
            public void run() {
                //TODO.
            }
        };
    }

    private Task createSortingTask(String[] data) {
        return new Task(data) {
            @Override
            public void run() {
                //TODO.
            }
        };
    }

    private void launchWorkers() {
        synchronized (workers) {
            workers.forEach(WorkerPrx::launch);
        }
    }

    @Override
    public Value getTask(Current current) {
        return queue.poll();
    }

    @Override
    public void addPartialResults(String[] array, Current current) {
        //TODO.
    }

    private void processResults() {
        //TODO.
    }

    private void shutdownWorkers() {
        synchronized (workers) {
            workers.forEach(WorkerPrx::shutdown);
        }
    }
}