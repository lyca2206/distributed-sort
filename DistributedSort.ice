module AppInterface
{
    ["java:type:java.util.ArrayList<String>"]
    sequence<string> ArrayList;

    dictionary<string, ArrayList> Map;

    class Task {
        long id;
        ArrayList data;
    };

    class GroupingTask extends Task {
        Map groups;
        int characters;
    };

    interface Worker
    {
        void launch();
        void ping();
        void shutdown();
    };

    interface Master
    {
        void signUp(string workerId, Worker* worker);
        Task getTask(string workerId);
        void addGroupingResults(string workerId, string taskId, Map groups);
        void addSortingResults(string workerId, string taskId, ArrayList array);
    };
};