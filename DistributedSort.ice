module AppInterface
{
    sequence<string> seqStr;

    enum TaskType {SORT, MERGE};

    class Task {
        TaskType type;
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
    };

    interface Master
    {
        void signUp(Worker* worker);
        Task getTask();
        void addPartialResults(seqStr array);
    };
};