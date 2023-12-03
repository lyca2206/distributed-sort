package sorter;

import java.util.concurrent.RecursiveAction;

public class MSDRadixSortTask extends RecursiveAction {

    private String[] array;
    private int lo;
    private int hi;
    private int d;
    private String[] aux; // auxiliary array for distribution --> Temp array for distribution
    private static final int R = 256; // radix --> Number of different letters in alphabet
    private static final int M = 15; // cutoff for small arrays --> Minimum size to use insertion sort

    public MSDRadixSortTask(String[] array) {
        this.array = array;
        this.lo = 0;
        this.hi = array.length - 1;
        this.aux = new String[array.length];
        this.d = 0;
    }

    private MSDRadixSortTask(String[] array, String[] aux, int lo, int hi, int d) {
        this.array = array;
        this.lo = lo;
        this.hi = hi;
        this.aux = aux;
        this.d = d;
    }

    private static int charAt(String s, int d) { // --> Returns -1 when string is shorter than d --> "aes" < "aesd", will put shorter strings first in sorted array
        if (d < s.length())
            return s.charAt(d);
        else
            return -1;
    }

    @Override
    protected void compute() {
        sort();
    }

    /**
     * Based on MSD.java from: https://algs4.cs.princeton.edu/51radix/
     */
    private void sort() { // Sort from array[lo] to array[hi], starting at the dth character.

        if (hi - lo <= M) { // Calls insertion sort if threshold is reached:  cost of recursion > cost of insertion
            Insertion.sort(array, lo, hi, d);
            return;
        }

        // Compute frequency counts. --> Count the number of appearances of each character in all the strings on the d position
        int[] count = new int[R + 2]; //Adds one to make room for end-of-string (-1) and another that its always 0 to denote begining index during recursion

        for (int i = lo; i <= hi; i++){
            count[charAt(array[i], d) + 2]++;
        }

        // Transform counts to indices. --> Calculates cumulative sum, this represents the indices in which strings with a certain character at position d will go during distribution
        for (int r = 0; r < R + 1; r++)
            count[r + 1] += count[r];

        // Distribute. --> Distributes string to its corresponding location (bucket) by the character at position d, sums one the count to tell the next string with that character where to go
        for (int i = lo; i <= hi; i++)
            aux[count[charAt(array[i], d) + 1]++ + lo] = array[i]; //+ lo  para que se pueda hacer a[i] = aux[i] (arraycopy)

        // Copy back --> Copies aux array to original array
        System.arraycopy(aux, lo, array, lo, hi + 1 - lo); // Not slow enough to parallelize

        MSDRadixSortTask[] tasks = new MSDRadixSortTask[R];

        // Recursively sort for each character value. --> Calls sort recursively, for each character
        for (int r = 0; r < R+1; r++) {
            int newLo = lo + count[r];
            int newHi = lo + count[r + 1] - 1;

            if(newHi - newLo + 1 > 1){ //If there are no elements or 1 element in group then subarray is already sorted
                MSDRadixSortTask task = new MSDRadixSortTask(array, aux, newLo, newHi,d + 1);
                task.fork();
                tasks[r] = task;
            }
        }

        for (MSDRadixSortTask task : tasks) {
            if (task != null) {
                task.join();
            }
        }
    }
}
