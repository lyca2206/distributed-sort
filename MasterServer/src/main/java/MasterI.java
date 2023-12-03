import AppInterface.*;
import com.zeroc.Ice.Current;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MasterI implements AppInterface.Master {
    private final List<WorkerPrx> prxList;

    private final ArrayDeque<Task> tasks = new ArrayDeque<>();

    private static final int MAX_THREADS = 4;

    // Ajusta según necesidad, este valor indica cuántas líneas por tarea se asignarán a cada worker
    private static final int LINES_PER_TASK = 250;


    public MasterI(List<WorkerPrx> prxList) {
        this.prxList = prxList;
    }

    @Override
    public void signUp(WorkerPrx worker, Current current) {
        prxList.add(worker);
    }

    public void initialize() throws IOException {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in))) {
            System.out.println("Enter the name of the file to be sorted. Be aware that you need to deploy the Workers first.");
            String fileName = "./" + br.readLine();

            //Create tasks
            createTasks(fileName);


            int numWorkers = calculateNumWorkers(getFileSize(fileName), getLineCount(fileName));
            System.out.println("Launching " + numWorkers + " workers...");
            launchWorkers(numWorkers);

            // Divide el archivo en tareas y envía a los workers


            // Continuar con la lógica de procesamiento de resultados...
        }
    }

    private void createTasks(String fileName) throws IOException {
        // Lee el archivo y divide en partes pequeñas
        //List<String[]> dataChunks = readAndFileAndCreateTasks(fileName, LINES_PER_TASK);

        readFileAndCreateTasks(fileName);

        // Genera tareas y las distribuye a los workers
        //distributeTasks(dataChunks);
    }

    private void readFileAndCreateTasks(String fileName) throws IOException {
        //List<String[]> dataChunks = new ArrayList<>();
        try (Stream<String> fileStream = Files.lines(Paths.get(fileName))) {
            List<String> lines = fileStream.collect(Collectors.toList());

            // Divide las líneas en partes pequeñas
            for (int i = 0; i < lines.size(); i += MasterI.LINES_PER_TASK) {
                int end = Math.min(i + MasterI.LINES_PER_TASK, lines.size());
                String[] chunk = lines.subList(i, end).toArray(new String[0]);
                //dataChunks.add(chunk);
                tasks.offer(new StringSortTask(TaskType.SORT,chunk));
            }
        }
        //return dataChunks;
    }

    private void distributeTasks(List<String[]> dataChunks) {
        int taskID = 1;
        for (String[] chunk : dataChunks) {
            StringSortTask sortTask = new StringSortTask(TaskType.SORT, chunk);
            distributeTask(sortTask, taskID++);
        }
    }

    private void distributeTask(StringSortTask task, int taskID) {
        // Implementa la lógica para distribuir la tarea a un worker específico
    }

    private void launchWorkers(int numWorkers) {
        List<WorkerPrx> selectedWorkers = new ArrayList<>();
        synchronized (prxList) {
            for (int i = 0; i < numWorkers && i < prxList.size(); i++) {
                selectedWorkers.add(prxList.get(i));
            }
        }

        selectedWorkers.forEach(WorkerPrx::launch);
    }

    private int calculateNumWorkers(long fileSize, long lineCount) {
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

        return totalThreads;
    }

    @Override
    public Task getTask(Current current) {
        return tasks.poll();
    }

    private final Queue<String []> partialResults = new ArrayDeque<>();

    @Override
    public void addPartialResults(String[] array, Current current) {

        String [] toMerge = partialResults.poll(); //check here for ending

        if(toMerge != null){
            tasks.push(new MergeTask(TaskType.MERGE,array, toMerge));
        }
        else{
            partialResults.offer(array);
        }
        //mergeResults(array); // Implement logic to process and merge partial results from workers
    }

    // Nueva función para implementar el algoritmo de ordenamiento MSD String Sort (Radix Sort)
    /*private void mergeResults(String[] array) {
        // Concatenar los resultados parciales
        String[] mergedArray = concatenateArrays(array);

        // Aplicar MSD String Sort (Radix Sort)
        msdStringSort(mergedArray, 0, mergedArray.length - 1, 0);

        // Continuar con la lógica de procesamiento de resultados...
        processResults(mergedArray);
    }

    private String[] concatenateArrays(String[] array) {
        // Implementa la lógica para concatenar resultados parciales
        // Puedes utilizar ArrayList para facilitar la concatenación
        List<String> resultList = new ArrayList<>();
        synchronized (prxList) {
            for (WorkerPrx workerPrx : prxList) {
                // Agrega lógica para obtener resultados parciales de cada worker y agregarlos a resultList
            }
        }
        return resultList.toArray(new String[0]);
    }
    */
    private void processResults(String[] array) {
        // Implementa la lógica para finalizar y presentar los resultados
        // Puedes imprimir el array ordenado o realizar otras acciones según tus necesidades
        System.out.println("Sorted Array: " + Arrays.toString(array));
    }

    private long getFileSize(String fileName) throws IOException {
        return Files.size(Paths.get(fileName));
    }

    private long getLineCount(String fileName) throws IOException {
        try (Stream<String> fileStream = Files.lines(Paths.get(fileName))) {
            return fileStream.count();
        }
    }

    private void processResults() {
        // Implement logic to finalize and present results
        // ...
    }

    private void shutdownWorkers() {
        synchronized (prxList) {
            prxList.forEach(WorkerPrx::shutdown);
        }
    }
}
