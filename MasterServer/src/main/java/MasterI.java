import AppInterface.StringSortTask;
import AppInterface.TaskType;
import AppInterface.WorkerPrx;
import com.zeroc.Ice.Current;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MasterI implements AppInterface.Master {
    private final List<WorkerPrx> prxList;

    private static final int MAX_THREADS = 4;

    // Ajusta según necesidad, este valor indica cuántas líneas por tarea se asignarán a cada worker
    private static final int LINES_PER_TASK = 100;


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

            int numWorkers = calculateNumWorkers(getFileSize(fileName), getLineCount(fileName));

            System.out.println("Launching " + numWorkers + " workers...");
            launchWorkers(numWorkers);

            // Divide el archivo en tareas y envía a los workers
            createAndDistributeTasks(fileName);

            // Continuar con la lógica de procesamiento de resultados...
        }
    }

    private void createAndDistributeTasks(String fileName) throws IOException {
        // Lee el archivo y divide en partes pequeñas
        List<String[]> dataChunks = readAndSplitFile(fileName, LINES_PER_TASK);

        // Genera tareas y las distribuye a los workers
        distributeTasks(dataChunks);
    }

    private List<String[]> readAndSplitFile(String fileName, int linesPerTask) throws IOException {
        List<String[]> dataChunks = new ArrayList<>();
        try (Stream<String> fileStream = Files.lines(Paths.get(fileName))) {
            List<String> lines = fileStream.collect(Collectors.toList());

            // Divide las líneas en partes pequeñas
            for (int i = 0; i < lines.size(); i += linesPerTask) {
                int end = Math.min(i + linesPerTask, lines.size());
                String[] chunk = lines.subList(i, end).toArray(new String[0]);
                dataChunks.add(chunk);
            }
        }
        return dataChunks;
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
    public StringSortTask getTask(Current current) {
        // Implementa la lógica para distribuir tareas a los workers
        String[] dataToSort = new String[0]; // Obtén los datos a ordenar, posiblemente desde el archivo
        return new StringSortTask(TaskType.SORT,dataToSort);
    }
    @Override
    public void addPartialResults(String[] array, Current current) {
        mergeResults(array); // Implement logic to process and merge partial results from workers
    }

    // Nueva función para implementar el algoritmo de ordenamiento MSD String Sort (Radix Sort)
    private void mergeResults(String[] array) {
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

    private void msdStringSort(String[] array, int low, int high, int digit) {
        final int R = 256; // Número de caracteres ASCII extendido

        if (high <= low || digit >= array[0].length()) {
            return;
        }

        // Utiliza un arreglo de contadores para contar la frecuencia de cada caracter
        int[] count = new int[R + 2];

        // Contar la frecuencia de cada caracter en la posición "digit"
        for (int i = low; i <= high; i++) {
            int charIndex = (array[i].length() > digit) ? array[i].charAt(digit) + 2 : 1;
            count[charIndex]++;
        }

        // Calcula las posiciones iniciales para cada caracter
        for (int r = 0; r < R + 1; r++) {
            count[r + 1] += count[r];
        }

        // Realiza la clasificación según el caracter en la posición "digit"
        String[] aux = new String[high - low + 1];
        for (int i = low; i <= high; i++) {
            int charIndex = (array[i].length() > digit) ? array[i].charAt(digit) + 1 : 0;
            aux[count[charIndex]++] = array[i];
        }

        // Copia los elementos ordenados de vuelta al array original
        System.arraycopy(aux, 0, array, low, aux.length);

        // Recursivamente ordena los subarrays para cada caracter
        for (int r = 0; r < R; r++) {
            msdStringSort(array, low + count[r], low + count[r + 1] - 1, digit + 1);
        }
    }

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
