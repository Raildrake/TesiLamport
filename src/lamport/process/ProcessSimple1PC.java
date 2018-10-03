package lamport.process;

import lamport.datastore.TimestampedRecord;
import lamport.payload.Payload;
import lamport.payload.Request;
import lamport.payload.TimestampedIDPayload;
import lamport.timestamps.UniqueTimestamp;

import java.net.Socket;
import java.util.LinkedList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ProcessSimple1PC extends Process<TimestampedIDPayload,UniqueTimestamp> {

    public ProcessSimple1PC(int port) {
        super(port,UniqueTimestamp.class);
        GetTimestamp().Set(0,port);

        GetDataStore().Add("A",new TimestampedRecord(2));
        GetDataStore().Add("B",new TimestampedRecord(3));
        GetDataStore().Add("C",new TimestampedRecord(5));
    }

    protected ReadWriteLock timestampLock=new ReentrantReadWriteLock();
    protected int failCount=0;
    protected int successCount=0;

    //TODO: crea superclasse con funzionalità transazionali che supporti diverti tipi di timestamp
    void SendPayload(UniqueTimestamp transactionTS, Socket s, Request req, String target, int val) {
        TimestampedIDPayload payload = new TimestampedIDPayload();
        payload.GetTimestamp().Set(transactionTS, false);
        payload.SetRequest(req);
        payload.SetTarget(target);
        payload.SetArg1(val);

        timestampLock.writeLock().lock();
        GetTimestamp().Add(1); //Il timestamp della transazione resterà quello per tutte le richieste della transazione, ma il timestamp del processo vogliamo che sia incrementato ad ogni messaggio comunque
        timestampLock.writeLock().unlock();

        Send(s, payload);
    }

    boolean ReadRequest(UniqueTimestamp transactionTS, Socket s, String target, TimestampedIDPayload output, LinkedList<TimestampedIDPayload> bufferedReads) {
        SendPayload(transactionTS,s,Request.READ,target,-1);
        TimestampedIDPayload res=WaitResponse(s,new Request[]{Request.SUCCESS_READ,Request.FAIL_READ,Request.BUFFERED_READ},target);
        if (res.GetRequest()==Request.BUFFERED_READ) bufferedReads.add(res);
        if (output!=null) output.CopyFrom(res);

        return res.GetRequest()!=Request.FAIL_READ;
    }
    boolean WriteRequest(UniqueTimestamp transactionTS, Socket s, String target, int value, TimestampedIDPayload output, LinkedList<TimestampedIDPayload> bufferedWrites) {
        SendPayload(transactionTS,s,Request.WRITE,target,value);
        TimestampedIDPayload res=WaitResponse(s,new Request[]{Request.SUCCESS_WRITE,Request.FAIL_WRITE,Request.BUFFERED_WRITE},target);
        if (res.GetRequest()==Request.BUFFERED_WRITE) bufferedWrites.add(res);
        if (output!=null) output.CopyFrom(res);

        return res.GetRequest()!=Request.FAIL_WRITE;
    }
    boolean PrewriteRequest(UniqueTimestamp transactionTS, Socket s, String target, TimestampedIDPayload output, LinkedList<TimestampedIDPayload> bufferedPrewrites) {
        SendPayload(transactionTS,s,Request.PREWRITE,target,-1);
        TimestampedIDPayload res=WaitResponse(s,new Request[]{Request.FAIL_PREWRITE,Request.BUFFERED_PREWRITE},target);
        if (res.GetRequest()==Request.BUFFERED_PREWRITE) bufferedPrewrites.add(res);
        if (output!=null) output.CopyFrom(res);

        return res.GetRequest()!=Request.FAIL_PREWRITE;
    }

    boolean ReadCancelRequest(UniqueTimestamp transactionTS, Socket s, String target) {
        SendPayload(transactionTS,s,Request.READ_CANCEL,target,-1);
        TimestampedIDPayload res=WaitResponse(s,new Request[]{Request.SUCCESS_READ_CANCEL},target);
        return res.GetRequest()==Request.SUCCESS_READ_CANCEL;
    }
    boolean WriteCancelRequest(UniqueTimestamp transactionTS, Socket s, String target) {
        SendPayload(transactionTS,s,Request.WRITE_CANCEL,target,-1);
        TimestampedIDPayload res=WaitResponse(s,new Request[]{Request.SUCCESS_WRITE_CANCEL},target);
        return res.GetRequest()==Request.SUCCESS_WRITE_CANCEL;
    }
    boolean PrewriteCancelRequest(UniqueTimestamp transactionTS, Socket s, String target) {
        SendPayload(transactionTS,s,Request.PREWRITE_CANCEL,target,-1);
        TimestampedIDPayload res=WaitResponse(s,new Request[]{Request.SUCCESS_PREWRITE_CANCEL},target);
        return res.GetRequest()==Request.SUCCESS_PREWRITE_CANCEL;
    }
    void WaitBuffer(LinkedList<TimestampedIDPayload> bufferList, Request[] possibleResponses) {
        while (bufferList.size() > 0) {
            TimestampedIDPayload buffered=bufferList.get(0);
            TimestampedIDPayload received = WaitResponse(buffered.GetUsedSocket(),possibleResponses,buffered.GetTarget());
            buffered.CopyFrom(received);
            bufferList.remove(buffered);
        }
    }

    @Override
    void OutputHandler() {
        while(true) {
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

        TimestampedIDPayload plA=new TimestampedIDPayload();
        TimestampedIDPayload plB=new TimestampedIDPayload();

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
    void PayloadReceivedHandler(TimestampedIDPayload payload) {
        ProcessRequest(payload.GetUsedSocket(),payload);
    }

    @Override
    void ProcessTimestamp(UniqueTimestamp timestamp) {

        timestampLock.writeLock().lock();

        UniqueTimestamp newT = UniqueTimestamp.Max(timestamp, GetTimestamp());
        newT.Add(1);
        GetTimestamp().Set(newT, true);

        timestampLock.writeLock().unlock();

    }

    void ProcessRequest(Socket s, TimestampedIDPayload payload) {
        //Siccome qui arrivano le richieste sul listen socket, possiamo aspettarci solo read o write
        if (payload.GetRequest()==Request.READ) {
            String n = payload.GetTarget();
            GetDataStore().Lock(n);
            TimestampedRecord record = (TimestampedRecord) GetDataStore().GetValue(n);

            timestampLock.readLock().lock();
            UniqueTimestamp ts=GetTimestamp();
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
            UniqueTimestamp ts=GetTimestamp();
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
