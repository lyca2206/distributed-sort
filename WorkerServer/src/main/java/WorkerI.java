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
        List<String> list = readFile(task.id);
        if (task != null) {
            if (task instanceof GroupingTask) {
                GroupingTask groupingTask = (GroupingTask) task;
                execute(() -> {
                    for (String string : list) {
                        String key = string.substring(0, groupingTask.characters);
                        if (!groupingTask.groups.containsKey(key)) { groupingTask.groups.put(key, new ArrayList<>()); }
                        groupingTask.groups.get(key).add(string);
                    }

                    Session session = createSession("swarch", masterHost, 22, "./auth/id_rsa", null);

                    groupingTask.groups.forEach((key, groupList) -> {
                        try {
                            String fileName = getFileName(key) + "_" + task.id;
                            writeFile(fileName, groupList);
                            copyLocalToRemote(session, "./temp", directory, fileName);
                        } catch (IOException | JSchException e) {
                            throw new RuntimeException(e);
                        }
                    });

                    session.disconnect();

                    masterPrx.addGroupingResults(id, groupingTask.id);
                });
            } else {
                execute(() -> {
                    list.sort(Comparator.naturalOrder());
                    try {
                        Session session = createSession("swarch", masterHost, 22, "./auth/id_rsa", null);
                        writeFile(task.id, list);
                        copyLocalToRemote(session, "./temp", directory, task.id);
                        session.disconnect();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    } catch (JSchException e) {
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
        BufferedWriter br = new BufferedWriter(new FileWriter(fileName));
        for (String string : list) {
            br.write(string + "\n");
        }
        br.close();
    }

    @Override
    public void shutdown(Current current) {
        isRunning = false;
        shutdown();
    }

    private static Session createSession(String user, String host, int port, String keyFilePath, String keyPassword) {
        try {
            JSch jsch = new JSch();

            if (keyFilePath != null) {
                if (keyPassword != null) {
                    jsch.addIdentity(keyFilePath, keyPassword);
                } else {
                    jsch.addIdentity(keyFilePath);
                }
            }

            Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");

            Session session = jsch.getSession(user, host, port);
            session.setConfig(config);
            session.connect();

            return session;
        } catch (JSchException e) {
            System.out.println(e);
            return null;
        }
    }
    private static void copyLocalToRemote(Session session, String from, String to, String fileName) throws JSchException, IOException {
        boolean ptimestamp = true;
        from = from + File.separator + fileName;

        // exec 'scp -t rfile' remotely
        String command = "scp " + (ptimestamp ? "-p" : "") + " -t " + to;
        Channel channel = session.openChannel("exec");
        ((ChannelExec) channel).setCommand(command);

        // get I/O streams for remote scp
        OutputStream out = channel.getOutputStream();
        InputStream in = channel.getInputStream();

        channel.connect();

        if (checkAck(in) != 0) {
            System.exit(0);
        }

        File _lfile = new File(from);

        if (ptimestamp) {
            command = "T" + (_lfile.lastModified() / 1000) + " 0";
            // The access time should be sent here,
            // but it is not accessible with JavaAPI ;-<
            command += (" " + (_lfile.lastModified() / 1000) + " 0\n");
            out.write(command.getBytes());
            out.flush();
            if (checkAck(in) != 0) {
                System.exit(0);
            }
        }

        // send "C0644 filesize filename", where filename should not include '/'
        long filesize = _lfile.length();
        command = "C0644 " + filesize + " ";
        if (from.lastIndexOf('/') > 0) {
            command += from.substring(from.lastIndexOf('/') + 1);
        } else {
            command += from;
        }

        command += "\n";
        out.write(command.getBytes());
        out.flush();

        if (checkAck(in) != 0) {
            System.exit(0);
        }

        // send a content of lfile
        FileInputStream fis = new FileInputStream(from);
        byte[] buf = new byte[1024];
        while (true) {
            int len = fis.read(buf, 0, buf.length);
            if (len <= 0) break;
            out.write(buf, 0, len); //out.flush();
        }

        // send '\0'
        buf[0] = 0;
        out.write(buf, 0, 1);
        out.flush();

        if (checkAck(in) != 0) {
            System.exit(0);
        }
        out.close();

        try {
            if (fis != null) fis.close();
        } catch (Exception ex) {
            System.out.println(ex);
        }

        channel.disconnect();
        session.disconnect();
    }

    public static int checkAck(InputStream in) throws IOException {
        int b = in.read();
        // b may be 0 for success,
        //          1 for error,
        //          2 for fatal error,
        //         -1
        if (b == 0) return b;
        if (b == -1) return b;

        if (b == 1 || b == 2) {
            StringBuffer sb = new StringBuffer();
            int c;
            do {
                c = in.read();
                sb.append((char) c);
            }
            while (c != '\n');
            if (b == 1) { // error
                System.out.print(sb.toString());
            }
            if (b == 2) { // fatal error
                System.out.print(sb.toString());
            }
        }
        return b;
    }

    private String getFileName(String group){
        StringBuilder fileName = new StringBuilder();
        for (int i = 0; i < group.length(); i++) {
            int letter = group.charAt(i);
            fileName.append(letter).append("_");
        }
        return fileName.toString();
    }

}