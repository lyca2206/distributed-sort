module AppInterface
{
    ["java:type:java.util.ArrayList<String>"]
    sequence<string> ArrayList;

    dictionary<string, ArrayList> Map;

    class Task {
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
        void signUp(string id, Worker* worker);
        Task getTask(string id);
        void addGroupingResults(Map groups);
        void addSortingResults(ArrayList array);
    };
};