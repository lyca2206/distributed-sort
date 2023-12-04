/**
 * Taken from: https://algs4.cs.princeton.edu/51radix/
 */
public class Insertion {
    public static void sort(String[] a, int lo, int hi, int d) { // Sort from a[lo] to a[hi], starting at the dth
                                                                 // character.
        for (int i = lo; i <= hi; i++)
            for (int j = i; j > lo && less(a[j], a[j - 1], d); j--)
                exch(a, j, j - 1);
    }

    //Inplace exchange
    private static void exch(String[] a, int e1, int e2) {
        String temp = a[e1];
        a[e1] = a[e2];
        a[e2] = temp;    
    }

    private static boolean less(String v, String w, int d) {
        for (int i = d; i < Math.min(v.length(), w.length()); i++)
            if (v.charAt(i) < w.charAt(i))
                return true;
            else if (v.charAt(i) > w.charAt(i))
                return false;
        return v.length() < w.length();
    }
}
