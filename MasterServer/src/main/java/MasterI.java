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
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class MasterI implements AppInterface.Master {
    private final Queue<Task> taskQueue;
    private final Map<String, WorkerPrx> workers;
    private final Map<String, Session> sessions;
    private final Map<String, Map<String, Task>> currentTasks;
    private final SortedSet<String> groupKeys;
    private final long pingMillis;
    private final String workerTemporalPath;

    private boolean isProcessing;
    private long processesAddingToResults;

    public MasterI(Queue<Task> taskQueue, Map<String, WorkerPrx> workers,
                   Map<String, Session> sessions, Map<String, Map<String, Task>> currentTasks,
                   SortedSet<String> groupKeys, long pingMillis, String workerTemporalPath) {
        this.taskQueue = taskQueue;
        this.workers = workers;
        this.sessions = sessions;
        this.currentTasks = currentTasks;
        this.groupKeys = groupKeys;
        this.pingMillis = pingMillis;
        this.workerTemporalPath = workerTemporalPath;
        isProcessing = false;
        processesAddingToResults = 0;
    }

    @Override
    public void signUp(String workerHost, WorkerPrx worker, Current current) {
        workers.put(workerHost, worker);
        currentTasks.put(workerHost, new ConcurrentHashMap<>());
        try {
            Session session = createSession(workerHost);
            session.connect();
            sessions.put(workerHost, session);
        }
        catch (JSchException e) { throw new RuntimeException(e); }
    }

    public void initialize(long batchSize) throws IOException {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in)))
        {
            System.out.println("Enter the name of the file to be sorted. Be aware that you need to deploy the Workers first.");
            String filePath = "./" + br.readLine();
            sortFile(filePath, batchSize);
        }
    }

    private void sortFile(String filePath, long batchSize) throws IOException {
        System.out.println("Sorting process has been started.");

        long startTime = System.nanoTime();
        launchWorkers();
        isProcessing = true;

        new Thread(this::checkVitalsOfWorkers).start();

        createTasksForGrouping(filePath, batchSize);
        waitForFinalization();

        mergeTaskGroups();

        createSortingTasks();
        waitForFinalization();

        isProcessing = false;
        shutdownWorkers();
        String outputFileName = writeResultIntoFile(filePath);
        long endTime = System.nanoTime();

        System.out.println("Sorting Time: " + (endTime - startTime) / 1000000 + "ms.");
        removeTemporalFiles();
        if (isFileSorted(outputFileName)) { System.out.println("The list has been sorted successfully."); }
        else { System.out.println("An error has happened."); }
    }

    private void launchWorkers() {
        System.out.println("Launching Workers...");
        workers.values().forEach(WorkerPrx::launch);
        System.out.println("Workers launched!");
    }

    private void checkVitalsOfWorkers() {
        System.out.println("Started checking Workers' Vitals.");

        while (isProcessing) {
            workers.keySet().forEach(this::checkVitals);

            try { Thread.sleep(pingMillis); }
            catch (InterruptedException e) { throw new RuntimeException(e); }
        }
        System.out.println("Finished checking Workers' Vitals.");
    }

    private void checkVitals(String workerKey) {
        WorkerPrx worker = workers.get(workerKey);

        try { worker.ping(); }
        catch (TimeoutException e) { resetWorkerTasks(workerKey); }
    }

    private void resetWorkerTasks(String workerKey) {
        System.out.println("Worker " + workerKey + " has been discarded (timeout).");

        Map<String, Task> currentWorkerTasks = currentTasks.remove(workerKey);
        taskQueue.addAll(currentWorkerTasks.values());

        workers.remove(workerKey);

        System.out.println("Worker " + workerKey + " tasks reassigned.");
    }

    private void createTasksForGrouping(String filePath, long batchSize) throws IOException {
        System.out.println("Creating Tasks for Grouping...");

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            long fileSize = getFileSize(filePath);
            long lineAmount = countLines(filePath);
            long taskAmount = fileSize / batchSize + 1;
            long taskSize = lineAmount / taskAmount + 1;
            //int keyLength = (int) (Math.log(taskAmount) / Math.log(26 * 2 + 10)) + 1;
            int keyLength = 1;

            for (long i = 0; i < taskAmount; i++) {
                ArrayList<String> dataChunk = getDataChunk(br, taskSize);
                createFileForChunkAndGatherKeys(dataChunk, String.valueOf(i), keyLength);
                Task task = new GroupingTask(String.valueOf(i), keyLength);
                taskQueue.add(task);
            }
        }
        System.out.println("Tasks for Grouping created!");
    }

    private long getFileSize(String fileName) throws IOException {
        return Files.size(Paths.get(fileName));
    }

    private long countLines(String fileName) throws IOException {
        try (Stream<String> fileStream = Files.lines(Paths.get(fileName))) {
            return fileStream.count();
        }
    }

    private ArrayList<String> getDataChunk(BufferedReader br, long taskSize) throws IOException {
        ArrayList<String> dataChunk = new ArrayList<>((int) taskSize);

        for (long i = 0; i < taskSize; i++) {
            String line = br.readLine();
            if (line == null) { return dataChunk; }
            dataChunk.add(line);
        }

        return dataChunk;
    }

    private void createFileForChunkAndGatherKeys(ArrayList<String> dataChunk, String fileName, int keyLength) throws IOException {
        String directory = "./temp/" + fileName;

        checkFileRestrictions(new File(directory));

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(directory))) {
            for (String line : dataChunk) {
                gatherKey(line, keyLength);
                bw.write(line);
                bw.newLine();
            }
        }
    }

    private void gatherKey(String line, int keyLength) {
        String key = line.substring(0, keyLength);
        groupKeys.add(key);
    }

    private void checkFileRestrictions(File file) throws IOException {
        if (file.exists() && !file.delete()) {
            throw new IOException("The file already exists and it couldn't be deleted.");
        }
        if (!file.createNewFile()) {
            throw new IOException("The file couldn't be created.");
        }
    }

    private void waitForFinalization() {
        System.out.println("Waiting...");
        while (true) {
            long currentTaskQuantity = countCurrentTasks();
            if (taskQueue.isEmpty() && currentTaskQuantity <= 0 && processesAddingToResults <= 0) {
                break;
            }
        }
        System.out.println("End waiting!");
    }

    private long countCurrentTasks() {
        long currentTaskQuantity = 0;

        for (Map<String, Task> map : currentTasks.values()) {
            currentTaskQuantity += map.size();
        }

        return currentTaskQuantity;
    }

    private void mergeTaskGroups() throws IOException {
        System.out.println("Merging groups generated by different solved tasks ...");

        for (String key : groupKeys) {
            String groupFileName = getGroupFileName(key);
            File[] allGroupFiles = getMatchingTemporaryFiles(groupFileName + ".*");

            File groupMergeFile = new File("./temp/" + groupFileName);
            checkFileRestrictions(groupMergeFile);

            BufferedWriter bw = new BufferedWriter(new FileWriter(groupMergeFile));
            for (File file : allGroupFiles) {
                writeFileIntoFile(bw, file);
            }
            bw.flush();
            bw.close(); //TODO. We might want to call the Garbage Collector.
        }
        System.out.println("Merged successfully!");
    }

    private void writeFileIntoFile(BufferedWriter bw, File file) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(file));

        String line = br.readLine();
        while (line != null) {
            bw.write(line);
            bw.newLine();
            line = br.readLine();
        }

        br.close();
    }

    private String getGroupFileName(String key) {
        StringBuilder groupFileName = new StringBuilder();

        for (int i = 0; i < key.length(); i++) {
            int character = key.charAt(i);
            groupFileName.append(character).append("_");
        }

        return groupFileName.toString();
    }

    private static File[] getMatchingTemporaryFiles(String regex) {
        File tempDirectory = new File("./temp/");

        if (!tempDirectory.isDirectory()) {
            throw new IllegalArgumentException(tempDirectory + "isn't a directory.");
        }

        final Pattern pattern = Pattern.compile(regex);
        return tempDirectory.listFiles((file) -> pattern.matcher(file.getName()).matches());
    }

    private void createSortingTasks() {
        System.out.println("Creating Sorting Tasks...");

        for (String key : groupKeys) {
            Task task = new Task(getGroupFileName(key));
            taskQueue.add(task);
        }
        System.out.println("Sorting Tasks created!");
    }

    private void shutdownWorkers() {
        System.out.println("Shutting Down Workers.");
        workers.values().forEach(WorkerPrx::shutdown);
        sessions.values().forEach(Session::disconnect);
        System.out.println("Workers Shut Down!");
    }

    private String writeResultIntoFile(String filePath) throws IOException {
         //String[] directories = filePath.split(File.pathSeparator); //TODO FILE SEPARATOR DOES NOT SPLIT PROPERLY THE FILE NAME

        String[] directories = filePath.split("/");
        String outputFileName = "sorted_" + directories[directories.length - 1];
        System.out.println("Writing Result into File: " + outputFileName);

        BufferedWriter bw = new BufferedWriter(new FileWriter("./" + outputFileName));
        for (String key : groupKeys) {
            String groupFilePath = "./temp/" + getGroupFileName(key);
            writeFileIntoFile(bw, new File(groupFilePath));
        }

        bw.close();

        System.out.println("Wrote results succesfully to " + outputFileName);
        return outputFileName;
    }

    private void removeTemporalFiles() {
        System.out.println("Removing temporal files...");
        for (File file : Objects.requireNonNull(new File("./temp/").listFiles())) {
            boolean notDeleted = !file.isDirectory() && !file.delete();
            if (notDeleted) { System.out.println("Couldn't delete file " + file + "."); }
        }
        System.out.println("Temporal files removed!");
    }

    private boolean isFileSorted(String outputFileName) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("./" + outputFileName));
        String previous = br.readLine();
        String line = br.readLine();

        while (line != null) {
            if (line.compareTo(previous) < 0) {
                br.close(); return false;
            }
            previous = line;
            line = br.readLine();
        }

        br.close(); return true;
    }

    @Override
    public Task getTask(String workerHost, Current current) {
        Task task = taskQueue.peek();
        if (task != null) {
            currentTasks.get(workerHost).put(task.key, task);
            taskQueue.remove();
            sendFileToWorker("./temp/" + task.key, workerTemporalPath, workerHost);
        }
        return task;
    }

    private void sendFileToWorker(String from, String to, String workerHost) {
        try {
            System.out.println("Sending file " + from + " to worker");
            long t1 = System.currentTimeMillis();
            File localFile = new File(from);

            Session session = sessions.get(workerHost);
            ChannelSftp channelSftp = (ChannelSftp) session.openChannel("sftp");
            channelSftp.connect();
            channelSftp.cd(to);
            channelSftp.put(new FileInputStream(localFile), localFile.getName());
            channelSftp.disconnect();

            long t2 = System.currentTimeMillis();
            System.out.println("File sent (" + (t2-t1) + " ms)");
        } catch (JSchException | SftpException | FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private Session createSession(String workerHost) throws JSchException {
        Session session = new JSch().getSession("swarch", workerHost, 22);
        session.setPassword("swarch");
        session.setConfig("StrictHostKeyChecking", "no");
        return session;
    }

    @Override
    public void addGroupingResults(String workerHost, String taskKey, Current current) {
        processesAddingToResults++;
        currentTasks.get(workerHost).remove(taskKey);
        processesAddingToResults--;
    }

    @Override
    public void addSortingResults(String workerHost, String taskKey, Current current) {
        processesAddingToResults++;
        currentTasks.get(workerHost).remove(taskKey);
        processesAddingToResults--;
    }
}