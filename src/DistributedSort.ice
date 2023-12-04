module AppInterface
{
    sequence<string> seqStr;

    enum TaskType {SORT, MERGE};

    class Task {
        TaskType type;
        int taskId;
    };

    class StringSortTask extends Task{
        seqStr array;
    };

    class MergeTask extends Task{
        seqStr array1;
        seqStr array2;
    };

    interface Worker
    {
        void launch();
        void shutdown();
        int ping();
    };

    interface Master
    {
        int signUp(Worker* worker);
        Task getTask(int workerId);
        void addPartialResults(int taskId, int workerId, seqStr array);
    };
};