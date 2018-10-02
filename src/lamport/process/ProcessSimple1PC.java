package lamport.process;

import lamport.datastore.TimestampedRecord;
import lamport.payload.Payload;
import lamport.payload.TimestampedIDPayload;
import lamport.timestamps.UniqueTimestamp;

import java.net.Socket;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ProcessSimple1PC extends Process<TimestampedIDPayload> {

    public ProcessSimple1PC(int port) {
        super(port);
        GetTimestamp().Set(0,port);

        GetDataStore().Add("A",new TimestampedRecord(2));
        GetDataStore().Add("B",new TimestampedRecord(3));
        GetDataStore().Add("C",new TimestampedRecord(5));
    }

    protected UniqueTimestamp timestamp=new UniqueTimestamp(); //TODO: classe apposita per timestamp per evitare ridondanza
    protected ReadWriteLock timestampLock=new ReentrantReadWriteLock();
    protected int failCount=0;
    protected int successCount=0;

    public UniqueTimestamp GetTimestamp() { return timestamp; }

    TimestampedIDPayload SendAndWaitResponse(UniqueTimestamp transactionTS, Socket s, Payload.Request req, String data, int val) {
        SendPayload(transactionTS,s,req,data,val);
        TimestampedIDPayload resPayload=Receive(s);
        ProcessTimestamp(resPayload);

        return resPayload;
    }
    void SendPayload(UniqueTimestamp transactionTS, Socket s, Payload.Request req, String data, int val) {
        TimestampedIDPayload payload=new TimestampedIDPayload();
        payload.GetTimestamp().Set(transactionTS,false);
        payload.SetRequest(req);
        payload.SetTarget(data);
        payload.SetArg1(val);

        timestampLock.writeLock().lock();
        GetTimestamp().Add(1); //Il timestamp della transazione resterà quello per tutte le richieste della transazione, ma il timestamp del processo vogliamo che sia incrementato ad ogni messaggio comunque
        timestampLock.writeLock().unlock();

        Send(s,payload);
    }


    @Override
    void OutputHandler() {
        while(true) {
            try {
                Thread.sleep(ThreadLocalRandom.current().nextInt(100, 3000));
            } catch (Exception e) { }

            while(!ExecuteTransaction())
            {
                failCount++;
                Log("Transaction failed! ("+successCount+"/"+(successCount+failCount)+")");
            } //ripeto finchè non riesco
            successCount++;
            Log("Transaction completed! ("+successCount+"/"+(successCount+failCount)+")");
        }
    }

    boolean ExecuteTransaction() {
        timestampLock.readLock().lock();
        UniqueTimestamp curTimestamp = GetTimestamp().clone();
        timestampLock.readLock().unlock();

        TimestampedIDPayload plA=SendAndWaitResponse(curTimestamp,GetRandomOutSocket(),Payload.Request.READ,"A",-1);
        if (plA.GetRequest()==Payload.Request.FAIL) return false;
        Log("Read A success!");
        TimestampedIDPayload plB=SendAndWaitResponse(curTimestamp,GetRandomOutSocket(),Payload.Request.READ,"B",-1);
        if (plB.GetRequest()==Payload.Request.FAIL) return false;
        Log("Read B success!");
        TimestampedIDPayload plWrite=SendAndWaitResponse(curTimestamp,GetRandomOutSocket(),Payload.Request.WRITE,"A",plA.GetArg1()+plB.GetArg1());
        if (plWrite.GetRequest()==Payload.Request.FAIL) return false;
        Log("Write A success!");
        return true;
    }

    @Override
    void PayloadReceivedHandler(Socket s, TimestampedIDPayload payload) {

        //elaboro prima il timestamp a prescindere
        ProcessTimestamp(payload);

        //ora elaboro la richiesta
        ProcessRequest(s,payload);
    }

    protected void ProcessTimestamp(TimestampedIDPayload payload) {
        timestampLock.writeLock().lock();

        UniqueTimestamp t = payload.GetTimestamp();
        UniqueTimestamp newT = UniqueTimestamp.Max(t, GetTimestamp());
        newT.Add(1);
        GetTimestamp().Set(newT, true);

        timestampLock.writeLock().unlock();
    }

    void ProcessRequest(Socket s, TimestampedIDPayload payload) {
        //Siccome qui arrivano le richieste sul listen socket, possiamo aspettarci solo read o write
        if (payload.GetRequest()==Payload.Request.READ) {
            String n = payload.GetTarget();
            GetDataStore().Lock(n);
            TimestampedRecord record = (TimestampedRecord) GetDataStore().GetValue(n);

            timestampLock.readLock().lock();
            UniqueTimestamp ts=GetTimestamp();
            timestampLock.readLock().unlock();

            if (record.GetW_TS().IsGreaterThan(payload.GetTimestamp())) { //fallito per conflitto RW, abbiamo TS<W-TS
                SendPayload(ts, s, Payload.Request.FAIL, payload.GetTarget(), -1);
            } else {
                SendPayload(ts, s, Payload.Request.SUCCESS_READ, payload.GetTarget(), record.GetValue());
                record.SetR_TS(payload.GetTimestamp());
            }

            GetDataStore().Unlock(n);
        } else if (payload.GetRequest()==Payload.Request.WRITE) {
            String n = payload.GetTarget();
            GetDataStore().Lock(n);

            TimestampedRecord record = (TimestampedRecord) GetDataStore().GetValue(n);

            timestampLock.readLock().lock();
            UniqueTimestamp ts=GetTimestamp();
            timestampLock.readLock().unlock();

            if (record.GetW_TS().IsGreaterThan(payload.GetTimestamp()) || record.GetR_TS().IsGreaterThan(payload.GetTimestamp())) { //fallito per conflitto RW/WW, abbiamo TS<W-TS oppure TS<R-TS
                SendPayload(ts, s, Payload.Request.FAIL, payload.GetTarget(), -1);
            } else {
                SendPayload(ts, s, Payload.Request.SUCCESS_WRITE, payload.GetTarget(), payload.GetArg1());
                record.SetValue(payload.GetArg1());
                record.SetW_TS(payload.GetTimestamp());
            }

            GetDataStore().Unlock(n);
        }
    }

}
