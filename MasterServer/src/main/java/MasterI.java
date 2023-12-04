import AppInterface.*;
import com.zeroc.Ice.Communicator;
import com.zeroc.Ice.Current;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MasterI implements AppInterface.Master {
    private final List<WorkerPrx> prxList;
    private final List<WorkerPrx> activeWorkersPrx = new ArrayList<>();
    private final List<WorkerPrx> inactiveWorkersPrx = new ArrayList<>();
    private final Queue<Task> tasks = new ConcurrentLinkedQueue<>();
    private final BlockingQueue<String []> partialResults = new LinkedBlockingQueue<>();
    private static final int MAX_THREADS = 4;
    private static final int LINES_PER_TASK = 1000000; //Number of Strings per task
    private final AtomicInteger taskIdCounter = new AtomicInteger();
    private final AtomicInteger completedTasksCounter = new AtomicInteger();
    private final Communicator communicator;
    private int totalTasks = 0;
    private int totalMergeTasks = 0;
    private long sortingStartTime;
    private int numRequiredWorkers;
    private final HashMap<Integer,Task> sentTasks;
    private final HashMap<Integer,Long> sentTasksTimes;
    private final HashMap<Integer,Set<Integer>> workersTasks; // worker id to task id
    private final HashMap<WorkerPrx,Integer> workersIds;
    private ExecutorService thPool;
    private long endTime = -1;
    private boolean isDoneSorting = false;
    private int workerIdCounter = 0;
    public MasterI(List<WorkerPrx> prxList, Communicator communicator) {
        this.prxList = prxList;
        inactiveWorkersPrx.addAll(prxList);
        sentTasksTimes = new HashMap<>();
        sentTasks = new HashMap<>();
        workersTasks = new HashMap<>();
        workersIds = new HashMap<>();
        this.communicator = communicator;
    }

    @Override
    public int signUp(WorkerPrx worker, Current current) {
        prxList.add(worker);

        if(activeWorkersPrx.size() < numRequiredWorkers){
            worker.launch();
        }

        workersTasks.put(workerIdCounter,ConcurrentHashMap.newKeySet());
        workersIds.put(worker,workerIdCounter);

        return  workerIdCounter++;
    }

    private void checkWorkers() {
        while (!isDoneSorting){
            for (WorkerPrx workerPrx:prxList) {

                try {
                    workerPrx.ping();
                }
                catch (Exception e) {
                    prxList.remove(workerPrx);

                    Iterator<WorkerPrx> it = inactiveWorkersPrx.iterator();

                    if(it.hasNext()){
                        WorkerPrx newWorker = it.next();
                        newWorker.launch();
                        inactiveWorkersPrx.remove(newWorker);
                        activeWorkersPrx.add(newWorker);
                    }

                    reassignTasksForDeadWorker(workerPrx);
                }
            }
            try {
                Thread.sleep(5000);
            } catch (InterruptedException ignored) {}
        }
    }

    private void reassignTasksForDeadWorker(WorkerPrx workerPrx){
        int workerId = workersIds.get(workerPrx);

        System.out.printf("Worker %d dead, reassigning tasks\n", workerId);

        Set<Integer> taskIds = workersTasks.get(workerId);

        for (int taskId:taskIds) {
            Task task = sentTasks.remove(taskId);
            sentTasksTimes.remove(taskId);
            tasks.add(task);
        }
    }

    public void initialize() throws IOException {
        boolean isFileOk = false;
        String line;
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        while(!isFileOk) {
            try{
                System.out.println("Enter the name of the file to be sorted. Be aware that you need to deploy the Workers first.");
                line = br.readLine();
                String fileName = "./" + line;

                //Create tasks
                readFileAndCreateSortTasks(fileName);
                isFileOk = true;
                int totalSortTasks = tasks.size();
                System.out.printf("Created %d sort tasks\n", totalSortTasks);
                totalMergeTasks = totalSortTasks - 1;
                totalTasks = totalSortTasks + totalMergeTasks;

                //Initialize master pool for background processes
                thPool = Executors.newFixedThreadPool(MAX_THREADS);
                thPool.execute(this::createMergeTasks);

                //Launch workers
                numRequiredWorkers = calculateNumWorkers(getLineCount(fileName));
                System.out.println("Launching " + numRequiredWorkers + " workers...");
                sortingStartTime = System.currentTimeMillis();
                launchWorkers(numRequiredWorkers);

                //Runs thread that checks workers health
                thPool.execute(this::checkWorkers);

                System.out.println("Launched workers!");
                System.out.println("Sorting started");
            } catch (IOException e) {
                System.out.println("Error " + e);
            }
        }
        br.close();
    }

    // Reads file and divides in LINES_PER_TASK parts
    private void readFileAndCreateSortTasks(String fileName) throws IOException {
        try (Stream<String> fileStream = Files.lines(Paths.get(fileName))) {
            List<String> lines = fileStream.collect(Collectors.toList());

            // Divides the file in small data chunks
            for (int i = 0; i < lines.size(); i += MasterI.LINES_PER_TASK) {
                int end = Math.min(i + MasterI.LINES_PER_TASK, lines.size());
                String[] chunk = lines.subList(i, end).toArray(new String[0]);
                tasks.offer(new StringSortTask(TaskType.SORT, taskIdCounter.incrementAndGet(),chunk));
            }
        }
    }

    private void launchWorkers(int numWorkers) {
        synchronized (prxList) {
            for (int i = 0; i < numWorkers && i < prxList.size(); i++) {
                activeWorkersPrx.add(prxList.get(i));
                inactiveWorkersPrx.remove(prxList.get(i));
            }
        }
        activeWorkersPrx.forEach(WorkerPrx::launch);
    }

    private int calculateNumWorkers(long lineCount) {/*
        // Número máximo de hilos para el servidor central
        int maxCentralThreads = 4;

        // Número de hilos para el worker en el mismo nodo del servidor central
        int workerThreads = 12;

        // Asumimos que cada worker puede manejar una cierta cantidad de datos (ajusta según tus necesidades)
        int dataPerWorker = 1000; // Por ejemplo, asumimos que cada worker puede manejar 1000 líneas de datos

        // Calculamos la cantidad de workers necesarios para manejar todos los datos
        int workersForData = (int) Math.ceil((double) lineCount / dataPerWorker);

        // Limitamos la cantidad de workers para que no exceda el número total de workers disponibles
        int totalWorkers = prxList.size();
        int workersToUse = Math.min(workersForData, totalWorkers);

        // Calculamos la cantidad total de hilos necesarios considerando los hilos para el servidor central y los workers
        int totalThreads = Math.min(maxCentralThreads, totalWorkers) + workersToUse * workerThreads;

        // Ajustamos el número de workers si hay más hilos disponibles que datos para procesar
        if (totalThreads > lineCount) {
            workersToUse = (int) Math.ceil((double) lineCount / workerThreads);
        }
        */
        //return workersToUse;
        return 12;
    }

    @Override
    public Task getTask(int workerId, Current current) {
        Task task = tasks.poll();

        if(task == null)
            return null;

        System.out.printf("Sent task %d/%d (%s) \n", task.taskId, totalTasks, task.type);
        sentTasks.put(task.taskId,task);
        sentTasksTimes.put(task.taskId,System.currentTimeMillis());

        workersTasks.get(workerId).add(task.taskId);

        return task;
    }

    private void createMergeTasks() {
        try {
            for (int i = 0; i < totalMergeTasks; i++){
                String[] partialResult1 = partialResults.take(); //Waits until an element gets in the queue
                String[] partialResult2 = partialResults.take();

                int taskId = taskIdCounter.incrementAndGet();

                tasks.offer(new MergeTask(TaskType.MERGE,taskId,partialResult1,partialResult2));
                System.out.printf("Created task %d/%d (%s) \n", taskId, totalTasks, TaskType.MERGE);
            }
        } catch (InterruptedException e) {
            System.out.println("Error " + e);
        }
    }

    @Override
    public void addPartialResults(int taskId, int workerId, String[] array, Current current) {

        if(!workersTasks.get(workerId).contains(taskId))
            return;

        partialResults.add(array);
        markTaskAsCompleted(taskId,workerId);

        if(completedTasksCounter.get() == totalTasks){
            endTime = System.currentTimeMillis();
            thPool.execute(this::processResults);
        }

    }

    private void markTaskAsCompleted(int taskId, int workerId) {
        Task completedTask = sentTasks.remove(taskId);
        workersTasks.get(workerId).remove(taskId);
        System.out.printf("Completed task %d/%d (%s) (%d ms)\n",taskId,totalTasks,completedTask.type,System.currentTimeMillis()-sentTasksTimes.get(taskId));
        completedTasksCounter.incrementAndGet();
    }

    public static boolean checkOrder (String[] array){
        for (int i = 0; i < array.length-1; i++) {
            if(array[i].compareTo(array[i+1])>0)
                return false;
        }
        return true;
    }

    private void processResults() {
        isDoneSorting = true;
        String[] sortedArray = partialResults.poll();

        System.out.println("Sorting finished");
        System.out.println("Time: " + (endTime- sortingStartTime) + " ms");
        assert sortedArray != null;
        System.out.println("Total strings sorted: " + sortedArray.length);
        System.out.println("Array is ordered: " + checkOrder(sortedArray));
        writeOutput(sortedArray);
        shutdownWorkers();
    }

    private void writeOutput(String[] array){
        String fileName = "ordered-array.txt";
        Long t1 = System.currentTimeMillis();
        System.out.println("Writing output to ./" + fileName);

        try (FileWriter writer = new FileWriter(fileName);
             BufferedWriter bw = new BufferedWriter(writer)) {
            for (String s : array) {
                bw.write(s + "\n");
            }
            bw.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        Long t2 = System.currentTimeMillis();
        System.out.println("Finish Writing to ./" + fileName + " (" + (t2-t1) + " ms)");
        isDoneSorting = true;
        thPool.shutdown();
        communicator.shutdown();
    }

    private long getLineCount(String fileName) throws IOException {
        try (Stream<String> fileStream = Files.lines(Paths.get(fileName))) {
            return fileStream.count();
        }
    }

    private void shutdownWorkers() {
        synchronized (prxList) {
            prxList.forEach(WorkerPrx::shutdown);
        }
    }
}
