package lamport.process;

import lamport.payload.Payload;
import lamport.payload.Request;
import lamport.timestamps.Timestamp;

import java.net.Socket;
import java.util.LinkedList;
import java.util.concurrent.ThreadLocalRandom;

public abstract class TransactionalProcess extends Process {

    public TransactionalProcess(int port) {
        super(port);
        InitData();
    }
    abstract void InitData();

    void SendPayload(Timestamp transactionTS, Socket s, Request req, String target, int val) {
        Payload payload = new Payload();
        payload.GetTimestamp().Set(transactionTS);
        payload.SetRequest(req);
        payload.SetTarget(target);
        payload.SetArg1(val);

        Send(s, payload);
    }

    protected boolean ReadRequest(Timestamp transactionTS, Socket s, String target, Payload output, LinkedList<Payload> bufferedReads) {
        SendPayload(transactionTS,s,Request.READ,target,-1);
        Payload res=WaitResponse(s,new Request[]{Request.SUCCESS_READ,Request.FAIL_READ,Request.BUFFERED_READ},target);
        if (res.GetRequest()==Request.BUFFERED_READ) bufferedReads.add(res);
        if (output!=null) output.CopyFrom(res);

        return res.GetRequest()!=Request.FAIL_READ;
    }
    protected boolean WriteRequest(Timestamp transactionTS, Socket s, String target, int value, Payload output, LinkedList<Payload> bufferedWrites) {
        SendPayload(transactionTS,s,Request.WRITE,target,value);
        Payload res=WaitResponse(s,new Request[]{Request.SUCCESS_WRITE,Request.FAIL_WRITE,Request.BUFFERED_WRITE},target);
        if (res.GetRequest()==Request.BUFFERED_WRITE) bufferedWrites.add(res);
        if (output!=null) output.CopyFrom(res);

        return res.GetRequest()!=Request.FAIL_WRITE;
    }
    protected boolean PrewriteRequest(Timestamp transactionTS, Socket s, String target, Payload output, LinkedList<Payload> bufferedPrewrites) {
        SendPayload(transactionTS,s,Request.PREWRITE,target,-1);
        Payload res=WaitResponse(s,new Request[]{Request.FAIL_PREWRITE,Request.BUFFERED_PREWRITE},target);
        if (res.GetRequest()==Request.BUFFERED_PREWRITE) bufferedPrewrites.add(res);
        if (output!=null) output.CopyFrom(res);

        return res.GetRequest()!=Request.FAIL_PREWRITE;
    }

    protected boolean ReadCancelRequest(Timestamp transactionTS, Socket s, String target) {
        SendPayload(transactionTS,s,Request.READ_CANCEL,target,-1);
        Payload res=WaitResponse(s,new Request[]{Request.SUCCESS_READ_CANCEL},target);
        return res.GetRequest()==Request.SUCCESS_READ_CANCEL;
    }
    protected boolean WriteCancelRequest(Timestamp transactionTS, Socket s, String target) {
        SendPayload(transactionTS,s,Request.WRITE_CANCEL,target,-1);
        Payload res=WaitResponse(s,new Request[]{Request.SUCCESS_WRITE_CANCEL},target);
        return res.GetRequest()==Request.SUCCESS_WRITE_CANCEL;
    }
    protected boolean PrewriteCancelRequest(Timestamp transactionTS, Socket s, String target) {
        SendPayload(transactionTS,s,Request.PREWRITE_CANCEL,target,-1);
        Payload res=WaitResponse(s,new Request[]{Request.SUCCESS_PREWRITE_CANCEL},target);
        return res.GetRequest()==Request.SUCCESS_PREWRITE_CANCEL;
    }
    protected void WaitBuffer(LinkedList<Payload> bufferList, Request[] possibleResponses) {
        while (bufferList.size() > 0) {
            Payload buffered=bufferList.get(0);
            Payload received = WaitResponse(buffered.GetUsedSocket(),possibleResponses,buffered.GetTarget());
            buffered.CopyFrom(received);
            bufferList.remove(buffered);
        }
    }

    boolean ExecuteTransaction() {
        timestampLock.readLock().lock();
        Timestamp curTimestamp = GetTimestamp().clone();
        timestampLock.readLock().unlock();

        LinkedList<Payload> bufferedReads = new LinkedList<>();
        LinkedList<Payload> bufferedWrites = new LinkedList<>();
        LinkedList<Payload> bufferedPrewrites = new LinkedList<>();

        try {
            if (GetArtificialDelayMax() > 0 && GetArtificialDelayMin() > 0)
                Thread.sleep(ThreadLocalRandom.current().nextInt(GetArtificialDelayMin(), GetArtificialDelayMax()));}catch(Exception e) {}
        try {

            boolean res=OnTransactionExecute(curTimestamp,bufferedPrewrites,bufferedReads,bufferedWrites);
            //Se la transazione è andata a buon fine siamo certi che ogni buffer remoto che abbia ospitato un prewrite l'avrà cancellato
            if (res) bufferedPrewrites.clear();
            return res;

        } finally {
            //potremmo aver fallito e alcuni buffer potrebbero essere rimasti contaminati, quindi li puliamo
            while (bufferedWrites.size() > 0) {
                Payload buffered = bufferedWrites.get(0);
                WriteCancelRequest(curTimestamp, buffered.GetUsedSocket(), buffered.GetTarget());
                bufferedWrites.removeFirst();
            }
            while (bufferedReads.size() > 0) {
                Payload buffered = bufferedReads.get(0);
                ReadCancelRequest(curTimestamp, buffered.GetUsedSocket(), buffered.GetTarget());
                bufferedReads.removeFirst();
            }
            while (bufferedPrewrites.size() > 0) {
                Payload buffered = bufferedPrewrites.get(0);
                PrewriteCancelRequest(curTimestamp, buffered.GetUsedSocket(), buffered.GetTarget());
                bufferedPrewrites.removeFirst();
            }
        }
    }

    abstract boolean OnTransactionExecute(Timestamp curTimestamp,
                                          LinkedList<Payload> bufferedPrewrites,
                                          LinkedList<Payload> bufferedReads,
                                          LinkedList<Payload> bufferedWrites);

    @Override
    void ProcessTimestamp(Timestamp timestamp) {

        timestampLock.writeLock().lock();

        Timestamp newT = Timestamp.Max(timestamp, GetTimestamp());
        newT.Add(1);
        GetTimestamp().Set(newT, true, true);

        timestampLock.writeLock().unlock();

    }
}
