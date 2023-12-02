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
        void signUp(string id, Worker* worker);
        Object getTask(string id);
        void addPartialResults(seqStr array);
    };
};