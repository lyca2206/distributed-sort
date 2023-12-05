import AppInterface.GroupingTask;
import AppInterface.MasterPrx;
import AppInterface.Task;
import com.jcraft.jsch.*;
import com.zeroc.Ice.Current;

import java.io.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class WorkerI extends ThreadPoolExecutor implements AppInterface.Worker {
    private final MasterPrx masterPrx;
    private final String id;
    private final String masterHost;
    private final String directory;
    private boolean isRunning;
    private static final String MASTER_TEMP_PATH = "/home/swarch/Documents/juanf-test-low-mem/MasterServer/temp";

    public WorkerI(int corePoolSize, int maximumPoolSize, long keepAliveTime,
                   TimeUnit unit, BlockingQueue<Runnable> workQueue, MasterPrx masterPrx, String id, String masterHost, String directory) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
        this.masterPrx = masterPrx;
        this.id = id;
        isRunning = false;
        this.masterHost = masterHost;
        this.directory = directory;
    }

    @Override
    public void launch(Current current) {
        isRunning = true;
        Thread thread = new Thread(this::startTaskPolling);
        thread.start();
    }

    @Override
    public void ping(Current current) {}

    private void startTaskPolling() {
        while (isRunning) {
            if (getActiveCount() < getMaximumPoolSize()) { getThenExecuteTask(); }
        }
    }

    private void getThenExecuteTask() {
        Task task = masterPrx.getTask(id);
        if (task != null) {
            List<String> list = readFile(task.id);
            if (task instanceof GroupingTask) {
                System.out.println("GroupingTask Received" + task.id);
                GroupingTask groupingTask = (GroupingTask) task;
                execute(() -> {

                    for (String string : list) {
                        String key = string.substring(0, groupingTask.characters);
                        if (!groupingTask.groups.containsKey(key)) { groupingTask.groups.put(key, new ArrayList<>()); }
                        groupingTask.groups.get(key).add(string);
                    }

                    groupingTask.groups.forEach((key, groupList) -> {
                        try {
                            String fileName = getFileName(key) + task.id;
                            writeFile(fileName, groupList);
                            sendFileToMaster(fileName);
                        } catch (IOException | JSchException | SftpException e) {
                            throw new RuntimeException(e);
                        }
                    });

                    masterPrx.addGroupingResults(id, groupingTask.id);
                });
            } else {
                execute(() -> {
                    list.sort(Comparator.naturalOrder());
                    try {
                        writeFile(task.id, list);
                        sendFileToMaster(task.id);
                    } catch (IOException | JSchException | SftpException e) {
                        throw new RuntimeException(e);
                    }
                    masterPrx.addSortingResults(id, task.id);
                });
            }
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

    private void writeFile(String fileName, List<String> list) throws IOException {
        File newFile = new File("./temp/" + fileName);

        if(newFile.exists() && !newFile.delete())
            throw new IOException("Field already exists and could not be deleted " + fileName);

        if (!newFile.createNewFile())
            throw new IOException("Could not create file " + fileName);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(newFile))) {
            for (String line : list) {
                writer.write(line);
                writer.newLine();
            }
        }
    }



    @Override
    public void shutdown(Current current) {
        isRunning = false;
        shutdown();
    }

    private String getFileName(String group){
        StringBuilder fileName = new StringBuilder();
        for (int i = 0; i < group.length(); i++) {
            int letter = group.charAt(i);
            fileName.append(letter).append("_");
        }
        return fileName.toString();
    }

    private void sendFileToMaster(String fileName) throws JSchException, FileNotFoundException, SftpException {
        //TODO REUTILIZAR LA SESIÓN PARA ENVIAR MULTIPLES ARCHIVOS
        File localFile = new File("./temp/"+fileName);

        Session session = new JSch().getSession("swarch",masterHost,22);
        session.setPassword("swarch");
        session.setConfig("StrictHostKeyChecking", "no");
        session.connect();

        ChannelSftp channel = (ChannelSftp)session.openChannel("sftp");
        channel.connect();
        channel.cd(MASTER_TEMP_PATH);
        channel.put(new FileInputStream(localFile),localFile.getName());
        channel.disconnect();

        session.disconnect();
    }

}