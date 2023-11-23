public class DistributedSortI implements AppInterface.DistributedSort {
    public void printString(String s, com.zeroc.Ice.Current current)
    {
        System.out.println(s);
    }
}
