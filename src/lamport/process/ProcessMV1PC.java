package lamport.process;

import lamport.datastore.MultiVersionRecord;
import lamport.payload.Payload;
import lamport.payload.Request;
import lamport.timestamps.Timestamp;

import java.util.LinkedList;

public class ProcessMV1PC extends TransactionalProcess {

    public ProcessMV1PC(int port) {
        super(port);
        GetTimestamp().Set(0,port);
    }

    @Override
    void InitData() {
        GetDataStore().Add("A", new MultiVersionRecord(2));
        GetDataStore().Add("B", new MultiVersionRecord(3));
        GetDataStore().Add("C", new MultiVersionRecord(5));
    }

    @Override
    void OutputHandler() {
        while(true) {
            GetStatCollector().StartAttempt();
            while(!ExecuteTransaction())
            {
                GetStatCollector().Fail();
                Log("Transaction failed! " + GetStatCollector().GetStats());
            } //ripeto finchè non riesco
            GetStatCollector().Success();
            Log("Transaction completed! " + GetStatCollector().GetStats());
        }
    }

    @Override
    boolean OnTransactionExecute(Timestamp curTimestamp,
                                 LinkedList<Payload> bufferedPrewrites,
                                 LinkedList<Payload> bufferedReads,
                                 LinkedList<Payload> bufferedWrites) {

        Payload plA=new Payload();
        Payload plB=new Payload();

        if (!ReadRequest(curTimestamp,GetRandomOutSocket(),"A",plA,null)) return false;
        Debug("Read A success!");

        if (!ReadRequest(curTimestamp,GetRandomOutSocket(),"B",plB,null)) return false;
        Debug("Read B success!");

        int newV=plA.GetArg1()+plB.GetArg1();
        if (!WriteRequest(curTimestamp,GetRandomOutSocket(),"A",newV,null,null)) return false;
        Debug("Write A success! ("+newV+")");

        return true;
    }


    @Override
    void PayloadReceivedHandler(Payload payload) {
        ProcessRequest(payload);
    }
    void ProcessRequest(Payload payload) {
        String n = payload.GetTarget();
        try {
            GetDataStore().Lock(n);

            timestampLock.readLock().lock();
            Timestamp curTS = GetTimestamp();
            timestampLock.readLock().unlock();

            MultiVersionRecord record = (MultiVersionRecord) GetDataStore().GetValue(n);

            if (payload.GetRequest() == Request.READ) {
                Debug("Received READ request for " + n + " with timestamp " + payload.GetTimestamp()+ " from "+payload.GetUsedSocket().getRemoteSocketAddress());

                //la eseguo sempre
                SendPayload(curTS, payload.GetUsedSocket(), Request.SUCCESS_READ, payload.GetTarget(), record.GetOldestVersionBefore(payload.GetTimestamp()));
                record.Read(payload.GetTimestamp());

            } else if (payload.GetRequest() == Request.WRITE) {
                Debug("Received WRITE request for " + n + " with timestamp " + payload.GetTimestamp() + " from " + payload.GetUsedSocket().getRemoteSocketAddress());

                Timestamp tEnd=record.GetFirstWriteAfter(payload.GetTimestamp());
                if (record.HasReadInInterval(payload.GetTimestamp(),tEnd)) {
                    SendPayload(curTS, payload.GetUsedSocket(), Request.FAIL_WRITE, payload.GetTarget(), -1);
                } else {
                    SendPayload(curTS, payload.GetUsedSocket(), Request.SUCCESS_WRITE, payload.GetTarget(), -1);
                    record.Write(payload.GetArg1(),payload.GetTimestamp());
                }

            }
        } finally {
            GetDataStore().Unlock(n);
        }
    }
}
