module AppInterface
{
    sequence<string> seqStr;

    ["java:implements:java.lang.Runnable"]
    class Task {
        seqStr data;
    };

    interface Worker
    {
        void launch();
        void shutdown();
    };

    interface Master
    {
        void signUp(Worker* worker);
        Object getTask();
        void addPartialResults(seqStr array);
    };
};