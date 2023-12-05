import AppInterface.GroupingTask;
import AppInterface.Task;
import AppInterface.WorkerPrx;
import com.jcraft.jsch.*;
import com.zeroc.Ice.Current;
import com.zeroc.Ice.TimeoutException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class MasterI implements AppInterface.Master {
    private final Queue<Task> queue; //task queque
    private final Map<String, WorkerPrx> workers; //Maps worker id to worker proxy
    private final Map<String, Map<String, Task>> currentTasks; //Maps worker id to the tasks is currently processing
    private final Map<String, List<String>> groups; // Maps
    private final long pingMillis;
    private boolean isProcessing;
    private long addingToResults;
    private long taskAmount;
    private static final String WORKERS_TEMP_PATH = "/home/swarch/Documents/juanf-test-low-mem/WorkerServer/temp";

    public MasterI(Queue<Task> queue, Map<String, WorkerPrx> workers,
                   Map<String, Map<String, Task>> currentTasks,
                   Map<String, List<String>> groups, long pingMillis) {
        this.queue = queue;
        this.workers = workers;
        this.currentTasks = currentTasks;
        this.groups = groups;
        this.pingMillis = pingMillis;
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

        System.out.println("Launching workers");

        launchWorkers();

        System.out.println("Starting worker ping thread");

        new Thread(this::startPingingWorkers).start();

        System.out.println("Creating grouping tasks");

        createGroupingTasks(fileName, batchSize);

        System.out.println("Serving Grouping tasks and Waiting for tasks to finish");

        doNextStepAfterFinalization();

        System.out.println("Merging grouping result");

        mergeGroupingTasksResults();

        System.out.println("Creating sorting tasks");

        createSortingTasks();

        System.out.println("Serving sorting tasks and waiting for tasks to finish");
        doNextStepAfterFinalization();
        isProcessing = false;

        shutdownWorkers();
        processAndServeResult(fileName);

        long endTime = System.nanoTime();

        System.out.println("Time: " + (endTime - startTime) + " ns.");
        for(File file: Objects.requireNonNull(new File("./temp/").listFiles())){
            if (!file.isDirectory()) {
                if (!file.delete()) {
                    System.out.println("Could not delete file " + file);
                }
            }
        }
        if (isSorted()) { System.out.println("The list has been sorted successfully."); }
        else { System.out.println("An error has happened."); }
    }

    private void launchWorkers() {
        workers.values().forEach(WorkerPrx::launch);
    }

    private void createGroupingTasks(String fileName, long batchSize) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            long fileSize = getFileSize(fileName);
            long listSize = getLineCount(fileName);
            long taskAmount = fileSize / batchSize + 1;
            this.taskAmount = taskAmount;
            long taskSize = listSize / taskAmount + 1;
            int characters = (int) (Math.log(taskAmount) / Math.log(26 * 2 + 10)) + 1;

            for (long i = 0; i < taskAmount; i++) {
                ArrayList<String> data = readData(br, taskSize);
                createGroupingFile(data, i);
                //TODO: Remove HashMap in Constructor.
                Task task = new GroupingTask(String.valueOf(i), new HashMap<>(), characters);
                queue.add(task);
            }
        }
    }

    private void createGroupingFile(ArrayList<String> data, long index) throws IOException {
        String fileName = "./temp/" + index;
        File newGroupFile = new File(fileName);

        if(newGroupFile.exists() && !newGroupFile.delete())
            throw new IOException("Field already exists and could not be deleted " + fileName);

        if (!newGroupFile.createNewFile())
            throw new IOException("Could not create file " + fileName);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            for (String line : data) {
                writer.write(line);
                writer.newLine();
            }
        }
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

    @Override
    public Task getTask(String workerId, Current current) {
        Task task = queue.poll();
        if (task != null) {
            try {
                currentTasks.get(workerId).put(task.id, task);
                sendFileToWorker(task.id,workerId);
            } catch (JSchException | FileNotFoundException | SftpException e) {
                throw new RuntimeException(e);
            }
        }
        return task;
    }

    private void sendFileToWorker(String fileName, String workerId) throws JSchException, FileNotFoundException, SftpException {
        File localFile = new File("./temp/"+fileName);

        Session session = new JSch().getSession("swarch",workerId,22);
        session.setPassword("swarch");
        session.setConfig("StrictHostKeyChecking", "no");
        session.connect();

        ChannelSftp channel = (ChannelSftp)session.openChannel("sftp");
        channel.connect();
        channel.cd(WORKERS_TEMP_PATH);
        channel.put(new FileInputStream(localFile),localFile.getName());
        channel.disconnect();

        session.disconnect();
    }

    public void mergeGroupingTasksResults() throws IOException {

        Set<String> groupsToMerge = groups.keySet(); //This is an ordered set

        for (String group : groupsToMerge) {
            String groupFileName = getFileName(group);

            File[] groupFiles = listTempFilesMatching(groupFileName+".*");

            BufferedWriter bw = new BufferedWriter(new FileWriter(groupFileName));
            for (File file : groupFiles) {
                BufferedReader br = new BufferedReader(new FileReader(file));

                String line = br.readLine();
                while (line != null) {
                    bw.write(line + "\n");
                    line = br.readLine();
                }
                br.close();
            }
            //TODO may be pertinent to call garbage collector
            bw.close();
        }
    }

    /**
     *
     * @param regex Regex
     * @return Array of files that match regex on ./temp/ folder
     */
    public static File[] listTempFilesMatching(String regex) {
        File root = new File("./temp/");

        if(!root.isDirectory()) {
            throw new IllegalArgumentException(root+" is no directory.");
        }

        final Pattern p = Pattern.compile(regex);
        return root.listFiles(file -> p.matcher(file.getName()).matches());
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
            try { Thread.sleep(pingMillis); }
            catch (InterruptedException e) { throw new RuntimeException(e); }
        }
    }

    private void resetTasks(String key) {
        System.out.println("Worker " + key + " has been discarded (timeout).");
        Map<String, Task> map = currentTasks.remove(key);
        queue.addAll(map.values());
        workers.remove(key);
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

    private void createSortingTasks() {
        groups.forEach((key, list) -> {
            Task task = new Task(key);
            queue.add(task);
        });
    }

    private List<String> readFile(String fileName) {
        List<String> list = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader("./temp/" + fileName))) {
            String line = br.readLine();
            while(line != null) {
                list.add(line);
                line = br.readLine();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return list;
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
    public void addGroupingResults(String workerId, String taskId, Current current) {
        addingToResults++;
        currentTasks.get(workerId).remove(taskId);
        addingToResults--;
    }

    @Override
    public void addSortingResults(String workerId, String taskId, Current current) {
        addingToResults++;
        currentTasks.get(workerId).remove(taskId);
        addingToResults--;
    }

    private void shutdownWorkers() {
        workers.values().forEach(WorkerPrx::shutdown);
    }
}
