package lamport.process;

import lamport.datastore.TimeBufferedRecord;
import lamport.payload.Payload;
import lamport.timestamps.Timestamp;

public class ProcessSimple2PC_Priority extends ProcessSimple2PC {

    public ProcessSimple2PC_Priority(int port) {
        super(port);
        GetTimestamp().Set(0,port,1);
    }

    @Override
    void OutputHandler() {
        while(true) {
            if (transactionSuccessCount+transactionFailCount>30000) continue;
            while (!ExecuteTransaction()) {
                transactionFailCount++;

                Log("Transaction failed! (" + transactionSuccessCount + "/" + (transactionSuccessCount + transactionFailCount) + ") (" + GetSuccessPercentString() + "%)");

                timestampLock.writeLock().lock();
                GetTimestamp().AddPriority(Math.max(transactionFailCount-transactionSuccessCount,1));
                timestampLock.writeLock().unlock();
            } //ripeto finchè non riesco
            transactionSuccessCount++;

            /*timestampLock.writeLock().lock();
            GetTimestamp().Add(1);
            timestampLock.writeLock().unlock();*/

            Log("Transaction completed! (" + transactionSuccessCount + "/" + (transactionSuccessCount + transactionFailCount) + ") (" + GetSuccessPercentString() + "%)");
        }
    }

    /*@Override
    void ProcessTimestamp(Timestamp timestamp) {

        timestampLock.writeLock().lock();

        Timestamp newT = Timestamp.Max(timestamp, GetTimestamp());
        //newT.Add(1);
        GetTimestamp().Set(newT, true, true);

        timestampLock.writeLock().unlock();

    }
    @Override
    void OnPayloadSent(Payload p) {}*/
}
