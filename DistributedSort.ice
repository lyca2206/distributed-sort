module AppInterface
{
    sequence<string> seqStr;

    ["java:implements:java.lang.Runnable"]
    class Task {
        void run();
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