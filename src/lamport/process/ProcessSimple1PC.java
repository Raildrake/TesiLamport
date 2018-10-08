package lamport.process;

import lamport.datastore.TimestampedRecord;
import lamport.payload.Payload;
import lamport.payload.Request;
import lamport.timestamps.Timestamp;

import java.net.Socket;
import java.util.LinkedList;

public class ProcessSimple1PC extends TransactionalProcess {

    public ProcessSimple1PC(int port) {
        super(port);
        GetTimestamp().Set(0,port);
    }
    @Override
    void InitData() {
        GetDataStore().Add("A",new TimestampedRecord(2));
        GetDataStore().Add("B",new TimestampedRecord(3));
        GetDataStore().Add("C",new TimestampedRecord(5));
    }

    @Override
    void OutputHandler() {
        while(true) {
            while(!ExecuteTransaction())
            {
                transactionFailCount++;
                Log("Transaction failed! ("+transactionSuccessCount+"/"+(transactionSuccessCount+transactionFailCount)+")");
            } //ripeto finchè non riesco
            transactionSuccessCount++;
            Log("Transaction completed! ("+transactionSuccessCount+"/"+(transactionSuccessCount+transactionFailCount)+")");
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
        Log("Read A success!");

        if (!ReadRequest(curTimestamp,GetRandomOutSocket(),"B",plB,null)) return false;
        Log("Read B success!");

        int newV=plA.GetArg1()+plB.GetArg1();
        if (!WriteRequest(curTimestamp,GetRandomOutSocket(),"A",newV,null,null)) return false;
        Log("Write A success! ("+newV+")");

        return true;
    }

    @Override
    void PayloadReceivedHandler(Payload payload) {
        ProcessRequest(payload.GetUsedSocket(),payload);
    }

    void ProcessRequest(Socket s, Payload payload) {
        //Siccome qui arrivano le richieste sul listen socket, possiamo aspettarci solo read o write
        if (payload.GetRequest()==Request.READ) {
            String n = payload.GetTarget();
            GetDataStore().Lock(n);
            TimestampedRecord record = (TimestampedRecord) GetDataStore().GetValue(n);

            timestampLock.readLock().lock();
            Timestamp ts=GetTimestamp();
            timestampLock.readLock().unlock();

            if (record.GetW_TS().IsGreaterThan(payload.GetTimestamp())) { //fallito per conflitto RW, abbiamo TS<W-TS
                SendPayload(ts, s, Request.FAIL_READ, payload.GetTarget(), -1);
            } else {
                SendPayload(ts, s, Request.SUCCESS_READ, payload.GetTarget(), record.GetValue());
                record.SetR_TS(payload.GetTimestamp());
            }

            GetDataStore().Unlock(n);
        } else if (payload.GetRequest()==Request.WRITE) {
            String n = payload.GetTarget();
            GetDataStore().Lock(n);

            TimestampedRecord record = (TimestampedRecord) GetDataStore().GetValue(n);

            timestampLock.readLock().lock();
            Timestamp ts=GetTimestamp();
            timestampLock.readLock().unlock();

            if (record.GetW_TS().IsGreaterThan(payload.GetTimestamp()) || record.GetR_TS().IsGreaterThan(payload.GetTimestamp())) { //fallito per conflitto RW/WW, abbiamo TS<W-TS oppure TS<R-TS
                SendPayload(ts, s, Request.FAIL_WRITE, payload.GetTarget(), -1);
            } else {
                SendPayload(ts, s, Request.SUCCESS_WRITE, payload.GetTarget(), payload.GetArg1());
                record.SetValue(payload.GetArg1());
                record.SetW_TS(payload.GetTimestamp());
            }

            GetDataStore().Unlock(n);
        }
    }

}
