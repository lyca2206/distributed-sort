module AppInterface
{
    ["java:type:java.util.ArrayList<String>"]
    sequence<string> ArrayList;

    dictionary<string, ArrayList> Map;

    class Task {
        string key;
    };

    class GroupingTask extends Task {
        long index;
        long step;
        int keyLength;
    };

    interface Worker
    {
        void launch();
        void ping();
        void shutdown();
    };

    interface Master
    {
        void signUp(string workerHost, Worker* worker);
        Task getTask(string workerHost);
        void addGroupingResults(string workerHost, string taskKey);
        void addSortingResults(string workerHost, string taskKey);
    };
};