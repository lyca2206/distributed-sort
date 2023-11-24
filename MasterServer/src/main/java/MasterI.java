import AppInterface.Task;
import AppInterface.WorkerPrx;
import com.zeroc.Ice.Current;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class MasterI implements AppInterface.Master {
    private final List<WorkerPrx> prxList;

    public MasterI(List<WorkerPrx> prxList) {
        this.prxList = prxList;
    }

    @Override
    public void signUp(WorkerPrx worker, Current current) {
        prxList.add(worker);
    }

    public void initialize() throws IOException {
        try(BufferedReader br = new BufferedReader(new InputStreamReader(System.in)))
        {
            System.out.println("Enter the name of the file to be sorted. Be aware that you need to deploy the Workers first.");
            String fileName = "./" + br.readLine();

            System.out.println(Arrays.toString(prxList.toArray()));
        }
    }

    private void createTasks(String fileName) {
    }

    private long getFileSize(String fileName) throws IOException {
        return Files.size(Paths.get(fileName));
    }

    private long getLineCount(String fileName) throws IOException {
        try (Stream<String> fileStream = Files.lines(Paths.get(fileName))) {
            return fileStream.count();
        }
    }

    private void launchWorkers() {
        synchronized (prxList) {
            prxList.forEach(WorkerPrx::launch);
        }
    }

    @Override
    public Task getTask(Current current) {
        return null;
    }

    @Override
    public void addPartialResults(String[] array, Current current) {
    }

    private void processResults() {
    }

    private void shutdownWorkers() {
        synchronized (prxList) {
            prxList.forEach(WorkerPrx::shutdown);
        }
    }
}