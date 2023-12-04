import AppInterface.GroupingTask;
import AppInterface.Task;
import AppInterface.WorkerPrx;
import com.zeroc.Ice.Current;
import com.zeroc.Ice.TimeoutException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public class MasterI implements AppInterface.Master {
    private final Queue<Task> queue;
    private final Map<String, WorkerPrx> workers;
    private final Map<String, Map<String, Task>> currentTasks;
    private final Map<String, List<String>> groups;
    private final long pingMilis;

    private boolean isProcessing;
    private long addingToResults;

    public MasterI(Queue<Task> queue, Map<String, WorkerPrx> workers,
                   Map<String, Map<String, Task>> currentTasks,
                   Map<String, List<String>> groups, long pingMilis) {
        this.queue = queue;
        this.workers = workers;
        this.currentTasks = currentTasks;
        this.groups = groups;
        this.pingMilis = pingMilis;
        isProcessing = false;
        addingToResults = 0;
    }

    @Override
    public void signUp(String workerId, WorkerPrx worker, Current current) {
        workers.put(workerId, worker);
        currentTasks.put(workerId, new ConcurrentHashMap<>());
    }

    public void initialize(long batchSize) throws IOException {
        try(BufferedReader br = new BufferedReader(new InputStreamReader(System.in)))
        {
            System.out.println("Enter the name of the file to be sorted. Be aware that you need to deploy the Workers first.");
            String fileName = "./" + br.readLine();
            sort(fileName, batchSize);
        }
    }

    private void sort(String fileName, long batchSize) throws IOException {
        System.out.println("Sorting has started.");

        long startTime = System.nanoTime();

        isProcessing = true;
        launchWorkers();
        new Thread(this::startPingingWorkers).start();

        createGroupingTasks(fileName, batchSize);
        doNextStepAfterFinalization();

        createSortingTasks();
        doNextStepAfterFinalization();
        isProcessing = false;

        shutdownWorkers();
        processAndServeResult(fileName);

        long endTime = System.nanoTime();

        System.out.println("Time: " + (endTime - startTime) + " ns.");
        if (isSorted()) { System.out.println("The list has been sorted successfully."); }
        else { System.out.println("An error has happened."); }
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
            try { Thread.sleep(pingMilis); }
            catch (InterruptedException e) { throw new RuntimeException(e); }
        }
    }

    private void resetTasks(String key) {
        System.out.println("Worker " + key + " has been discarded (timeout).");
        Map<String, Task> map = currentTasks.remove(key);
        queue.addAll(map.values());
        workers.remove(key);
    }

    private void createGroupingTasks(String fileName, long batchSize) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            long fileSize = getFileSize(fileName);
            long listSize = getLineCount(fileName);
            long taskAmount = fileSize / batchSize + 1;
            long taskSize = listSize / taskAmount + 1;
            int characters = (int) (Math.log(taskAmount) / Math.log(26 * 2 + 10)) + 1;

            for (long i = 0; i < taskAmount; i++) {
                ArrayList<String> data = readData(br, taskSize);

                // Crear el archivo temporal
                String tempFileName = createTempFile(data, i);

                // TODO: Actualiza la creación de la tarea para utilizar el nuevo archivo temporal
                Task task = new GroupingTask(String.valueOf(i), new HashMap<>(), characters);
                queue.add(task);
            }
        }
    }

    private String createTempFile(ArrayList<String> data, long index) throws IOException {
        String fileName = "/temp/" + index;
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            for (String line : data) {
                writer.write(line);
                writer.newLine();
            }
        }
        return fileName;
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
        groups.forEach((key, list) -> {
            //TODO.
            Task task = new Task(key);
            queue.add(task);
        });
    }

    private void processAndServeResult(String fileName) throws IOException {
        String[] directory = fileName.split("/");
        String outFile = "sorted_" + directory[directory.length - 1];

        Set<String> groupsToMerge = groups.keySet(); //This is an ordered set
        Iterator<String> groupsIterator = groupsToMerge.iterator();


        BufferedWriter bw = new BufferedWriter(new FileWriter(outFile));
        while(groupsIterator.hasNext()) {
            String group = groupsIterator.next();

            String file = "./temp/" + getFileName(group);

            BufferedReader br = new BufferedReader(new FileReader(file));

            String line = br.readLine();

            while(line != null){
                bw.write(line + "\n");
                line = br.readLine();
            }

            br.close();
        }
        bw.close();
    }

    /**
     * Given a group such as "aa" returns its corresponding filename represented in integer of char
     * "aa" return "97_97_", "a" returns "97_"
     */
    private String getFileName(String group){
        StringBuilder fileName = new StringBuilder();
        for (int i = 0; i < group.length(); i++) {
            int letter = group.charAt(i);
            fileName.append(letter).append("_");
        }
        return fileName.toString();
    }


    private boolean isSorted() {
        String previous = null;
        for (List<String> list : groups.values()) {
            for (String string : list) {
                if (previous == null) { previous = string; }
                else if (string.compareTo(previous) < 0) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public Task getTask(String workerId, Current current) {
        Task task = queue.poll();
        if (task != null) { currentTasks.get(workerId).put(task.id, task); }
        return task;
    }

    @Override
    public void addGroupingResults(String workerId, String taskId, Current current) {
        currentTasks.get(workerId).remove(taskId);
    }

    @Override
    public void addSortingResults(String workerId, String taskId, Current current) {
        currentTasks.get(workerId).remove(taskId);
    }

    private void shutdownWorkers() {
        workers.values().forEach(WorkerPrx::shutdown);
    }
}
