package merger;

import java.util.concurrent.RecursiveTask;

public class MergeArraysTask extends RecursiveTask<String[]> {

    @Override
    public String toString() {
        return "MergeArraysTask [ini1=" + ini1 + ", end1=" + end1 + ", ini2=" + ini2 + ", end2=" + end2 + ", iniAux="
                + iniAux + "]";
    }

    private String[] array;
    private String[] aux;
    private int ini1;
    private int end1;
    private int ini2;
    private int end2;
    private int iniAux;

    private static final int SEQ_MERGE_THRESHOLD = 100;

    public MergeArraysTask(String[] array1, String[] array2) {
        int mergedArraySize = array2.length + array1.length;

        ini1 = 0;
        end1 = array1.length - 1;
        ini2 = array1.length;
        end2 = mergedArraySize - 1;
        iniAux = 0;

        array = new String[mergedArraySize];
        aux = new String[mergedArraySize];

        System.arraycopy(array1,0,array,0,array1.length);
        System.arraycopy(array2,0,array,array1.length,array2.length);

    }

    private MergeArraysTask(String[] array, String[] aux, int ini1, int end1, int ini2, int end2, int iniAux) {
        this.array = array;
        this.aux = aux;
        this.ini1 = ini1;
        this.end1 = end1;
        this.ini2 = ini2;
        this.end2 = end2;
        this.iniAux = iniAux;
    }

    @Override
    protected String[] compute() {
        merge();
        return aux;
    }

    /**
     * <a href="https://dl.ebooksworld.ir/books/Introduction.to.Algorithms.4th.Leiserson.Stein.Rivest.Cormen.MIT.Press.9780262046305.EBooksWorld.ir.pdf">...</a>
     */
    private void merge() {

        if (end1 - ini1 < SEQ_MERGE_THRESHOLD){ // Base case (sequential merge)
            sequentialMerge();
            return;
        }

        if (end1 - ini1 < end2 - ini2) { //Maintains balance, to found pivot on bigger array
            int ini1tmp = ini1;
            int end1tmp = end1;
            ini1 = ini2;
            end1 = end2;
            ini2 = ini1tmp;
            end2 = end1tmp;
        }

        int mid1 = (ini1 + end1)/2;
        String mid1Value = array[mid1];

        int mid2 = findSplitPoint(ini2,end2,mid1Value);

        int midAux = iniAux + (mid1 - ini1) + (mid2 - ini2);
        aux[midAux] = mid1Value;

        MergeArraysTask mergeTask1 = new MergeArraysTask(array,aux,ini1,mid1-1,ini2,mid2-1,iniAux);
        mergeTask1.fork();

        MergeArraysTask mergeTask2 = new MergeArraysTask(array,aux,mid1+1,end1,mid2, end2,midAux+1);
        mergeTask2.fork();

        mergeTask1.join();
        mergeTask2.join();
    }

    /**
     * <a href="https://www.baeldung.com/java-merge-sorted-arrays">...</a>
     * <a href="https://dl.ebooksworld.ir/books/Introduction.to.Algorithms.4th.Leiserson.Stein.Rivest.Cormen.MIT.Press.9780262046305.EBooksWorld.ir.pdf">...</a>
     */
    private void sequentialMerge(){

        int current1 = ini1;
        int current2 = ini2;
        int currentAux = iniAux;

        while(current1 <= end1 && current2 <= end2) {
            if (array[current1].compareTo(array[current2]) <= 0) {
                aux[currentAux++] = array[current1++];
            } else {
                aux[currentAux++] = array[current2++];
            }
        }

        while (current1 <= end1) {
            aux[currentAux++] = array[current1++];
        }

        while (current2 <= end2) {
            aux[currentAux++] = array[current2++];
        }
    }

    private int findSplitPoint(int ini, int end, String value){
        int low = ini;
        int high = end + 1;
        int mid;

        while(low < high){
            mid = (low + high)/2;

            if(value == null){
                System.out.println("CAUSE value=null" + this);
            }

            if(array[mid] == null){
                System.out.println("CAUSE mid=" + mid + " array[mid]=null " + this );
                System.out.println("low = " + low + " high = " + high);
            }

            if(value.compareTo(array[mid]) <= 0){
                high = mid;
            }
            else{
                low = mid + 1;
            }
        }
        return low;
    }
}
