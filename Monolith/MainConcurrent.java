import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ForkJoinPool;

public class MainConcurrent{

    //static final String[] wordsArray = {"Apple","Banana","Orange","Grapes","Pineapple","Strawberry","Watermelon","Kiwi","Mango","Peach","Lemon","Cherry","Blueberry","Blackberry","Raspberry","Pear","Plum","Apricot","Cranberry","Coconut","Avocado","Pomegranate","Guava","Fig","Papaya","Lychee","Passionfruit","Tangerine","Melon","Nectarine","Persimmon","Lime","Dragonfruit","Date","Starfruit","Kumquat","Quince","Honeydew","Cantaloupe","Jackfruit","Mulberry","Mandarin","Elderberry","Boysenberry","Currant","Gooseberry","Rambutan","Feijoa","Bilberry","Acai","Durian","Carambola","Loquat","Cherimoya","Plantain","Ackee","Cactus","Barberry","Tamarillo","Ugli","Salak","Mangosteen","Feijoa","Chayote","Sapodilla","Soursop","Tamarind","Yuzu","Jujube","Kiwano","Longan","Miraclefruit","Pawpaw","Pepino","Persimmon","Pricklypear","Salmonberry","Sapote","Tayberry","Thimbleberry","Ugni","Vanilla","Feijoa","Yali","Zucchini","Aonori","Bilimbi","Calamondin","Damson","Emblica","Fuyu","Gac","Hornedmelon"};

    public static void main(String[] args) throws IOException, InterruptedException {    

        ForkJoinPool pool = new ForkJoinPool();        

        long t1 = System.currentTimeMillis();

        BufferedReader br = new BufferedReader(new FileReader("C:/Users/juanf/Documents/Repositories/232/arq-soft/impl-msd-radix-sort/data/ejemplo1.dat"));
        List<String> words = new ArrayList<>();

        String line = br.readLine();

        while (line != null){
            words.add(line);
            line = br.readLine();
        }
        
        br.close();

        long t2 = System.currentTimeMillis();

        System.out.println("Time to read file: " + (t2-t1) + " ms");

        long t3 = System.currentTimeMillis();

        String[] wordsArray = words.toArray(new String[0]);

        long t4 = System.currentTimeMillis();      

        System.out.println("Time to convert list to array: " + (t4-t3) + " ms");
        
        long t5 = System.currentTimeMillis();      

        String[] origArray = wordsArray.clone();

        long t6 = System.currentTimeMillis();

        System.out.println("Time to clone list: " + (t6-t5) + " ms");
        
        long t7 = System.currentTimeMillis();
        
        MSDRadixSortTask task = new MSDRadixSortTask(wordsArray);
        pool.invoke(task);        

        long t8 = System.currentTimeMillis();
        
        System.out.println("Time to sort parallel: " + (t8 - t7) + " ms");  

        long t9 = System.currentTimeMillis();

        writeOutput(wordsArray);

        long t10 = System.currentTimeMillis();

        System.out.println("Total read + toArray + sort + write: " + (t8 - t7 + t4 - t3 + t2 - t1 + t10 - t9) + " ms");

        System.out.println("Number of strings: " + wordsArray.length);
        System.out.println("Is Ordered: " + checkOrder(wordsArray));
        System.out.println("Elements are correct: " + haveSameElementCounts(origArray, wordsArray));
        //System.out.println(Arrays.toString(wordsArray));
    }
    
    public static boolean checkOrder (String[] array){
        for (int i = 0; i < array.length-1; i++) {
            if(array[i].compareTo(array[i+1])>0)
                return false;
        }
        return true;
    }

    public static boolean haveSameElementCounts(String[] array1, String[] array2) {

        // Check if the arrays have the same length
        if (array1.length != array2.length) {
            return false;
        }
    
        // Create a HashMap to store the element counts for the first array
        HashMap<String, Integer> elementCounts1 = new HashMap<>();
        for (String element : array1) {
            if (!elementCounts1.containsKey(element)) {
                elementCounts1.put(element, 1);
            } else {
                elementCounts1.put(element, elementCounts1.get(element) + 1);
            }
        }
    
        // Create a HashMap to store the element counts for the second array
        HashMap<String, Integer> elementCounts2 = new HashMap<>();
        for (String element : array2) {
            if (!elementCounts2.containsKey(element)) {
                elementCounts2.put(element, 1);
            } else {
                elementCounts2.put(element, elementCounts2.get(element) + 1);
            }
        }
    
        // Compare the element counts for each array
        for (String element : elementCounts1.keySet()) {
            if (!elementCounts1.get(element).equals(elementCounts2.get(element))) {
                return false;
            }
        }
    
        return true;
    }

    public static void writeOutput(String[] array){
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
    }
}