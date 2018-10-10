package lamport.process;

import lamport.payload.Payload;
import lamport.timestamps.Timestamp;

public class ProcessMV2PC_Priority extends ProcessMV2PC {

    public ProcessMV2PC_Priority(int port) {
        super(port);
        GetTimestamp().Set(0,port,1);
    }

    @Override
    void OutputHandler() {
        while(true) {
            GetStatCollector().StartAttempt();
            while (!ExecuteTransaction()) {
                GetStatCollector().Fail();

                Log("Transaction failed! " + GetStatCollector().GetStats());

                timestampLock.writeLock().lock();
                GetTimestamp().AddPriority(Math.max(GetStatCollector().GetFailCount()-GetStatCollector().GetSuccessCount(),1));
                GetTimestamp().Add(1);
                timestampLock.writeLock().unlock();
            } //ripeto finchè non riesco
            GetStatCollector().Success();

            timestampLock.writeLock().lock();
            GetTimestamp().Add(1);
            timestampLock.writeLock().unlock();

            Log("Transaction completed! " + GetStatCollector().GetStats());
        }
    }

    @Override
    void ProcessTimestamp(Timestamp timestamp) {

        timestampLock.writeLock().lock();

        Timestamp newT = Timestamp.Max(timestamp, GetTimestamp());
        //newT.Add(1);
        GetTimestamp().Set(newT, true, true);

        timestampLock.writeLock().unlock();

    }
    @Override
    void OnPayloadSent(Payload p) {}

}
