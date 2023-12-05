import AppInterface.GroupingTask;
import AppInterface.MasterPrx;
import AppInterface.Task;
import com.jcraft.jsch.*;
import com.zeroc.Ice.Current;

import java.io.*;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class WorkerI extends ThreadPoolExecutor implements AppInterface.Worker {
    private final MasterPrx masterPrx;
    private final String masterHost;
    private final String masterTemporalPath;
    private final String workerHost;

    private boolean isRunning;

    public WorkerI(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
                   BlockingQueue<Runnable> workQueue, MasterPrx masterPrx, String masterHost,
                   String masterTemporalPath, String workerHost) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
        this.masterPrx = masterPrx;
        this.masterHost = masterHost;
        this.masterTemporalPath = masterTemporalPath;
        this.workerHost = workerHost;
        isRunning = false;
    }

    @Override
    public void launch(Current current) {
        isRunning = true;
        new Thread(this::startTaskPolling).start();
    }

    @Override
    public void ping(Current current) {}

    private void startTaskPolling() {
        while (isRunning) {
            if (getActiveCount() < getMaximumPoolSize()) { getThenExecuteTask(); }
        }
    }

    private void getThenExecuteTask() {
        Task task = masterPrx.getTask(workerHost);
        if (task != null) {
            List<String> list = readFile(task.key);
            if (task instanceof GroupingTask) { execute(() -> taskForGrouping(list, (GroupingTask) task)); }
            else { execute(() -> taskForSorting(list, task)); }
        }
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

    private void taskForGrouping(List<String> list, GroupingTask task) {
        System.out.println("Grouping Task Received.");

        Map<String, List<String>> groups = separateListIntoGroups(list, task.keyLength);
        groups.forEach((key, groupList) -> createFileForGroupAndSendToMaster(task.key, key, groupList));

        masterPrx.addGroupingResults(workerHost, task.key);
    }

    private Map<String, List<String>> separateListIntoGroups(List<String> list, int keyLength) {
        Map<String, List<String>> groups = new HashMap<>();

        for (String string : list) {
            String key = string.substring(0, keyLength);
            if (!groups.containsKey(key)) { groups.put(key, new ArrayList<>()); }
            groups.get(key).add(string);
        }

        return groups;
    }

    private void createFileForGroupAndSendToMaster(String taskKey, String key, List<String> groupList) {
        try {
            String groupFileName = getGroupFileName(key) + taskKey;
            createFile(groupFileName, groupList);
            sendFileToMaster(groupFileName, masterTemporalPath, masterHost);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String getGroupFileName(String key) {
        StringBuilder groupFileName = new StringBuilder();

        for (int i = 0; i < key.length(); i++) {
            int character = key.charAt(i);
            groupFileName.append(character).append("_");
        }

        return groupFileName.toString();
    }

    private void createFile(String fileName, List<String> data) throws IOException {
        String filePath = "./temp/" + fileName;

        checkFileRestrictions(new File(filePath));

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(filePath))) {
            for (String line : data) {
                bw.write(line);
                bw.newLine();
            }
        }
    }

    private void checkFileRestrictions(File file) throws IOException {
        if (file.exists() && !file.delete()) {
            throw new IOException("The file already exists and it couldn't be deleted.");
        }
        if (!file.createNewFile()) {
            throw new IOException("The file couldn't be created.");
        }
    }

    private void sendFileToMaster(String from, String to, String masterHost) {
        //TODO. We gotta reuse the session to send various files.
        try {
            File localFile = new File(from);

            Session session = createSession(masterHost);
            session.connect();

            ChannelSftp channelSftp = (ChannelSftp) session.openChannel("sftp");
            channelSftp.connect();
            channelSftp.cd(to);
            channelSftp.put(new FileInputStream(localFile), localFile.getName());
            channelSftp.disconnect();

            session.disconnect();
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

    private void taskForSorting(List<String> list, Task task) {
        list.sort(Comparator.naturalOrder());
        try {
            createFile(task.key, list); //The FileName has been formatted from Master, hence why we use 'task.key'.
            sendFileToMaster(task.key, masterTemporalPath, masterHost);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        masterPrx.addSortingResults(workerHost, task.key);
    }

    @Override
    public void shutdown(Current current) {
        isRunning = false;
        shutdown();
    }
}