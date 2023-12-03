module AppInterface
{
    sequence<string> seqStr;

    interface Worker
    {
        void launch();
        void addGroupResults(seqStr array);
        void addSortResults(seqStr array);
        void shutdown();
    };

    ["java:implements:java.lang.Runnable"]
    class Task {
        Worker* worker;
        seqStr data;
    };

    interface Master
    {
        void signUp(string id, Worker* worker);
        Task getTask(string id);
        void addGroupResults(seqStr array);
        void addSortResults(seqStr array);
    };
};