package lamport.process;

import lamport.datastore.TimeBufferedRecord;
import lamport.payload.Payload;
import lamport.payload.Request;
import lamport.timestamps.Timestamp;

import java.util.LinkedList;
import java.util.concurrent.ThreadLocalRandom;

public class ProcessSimple2PC extends TransactionalProcess {

    public ProcessSimple2PC(int port) {
        super(port);
        GetTimestamp().Set(0,port);
    }
    @Override
    void InitData() {
        GetDataStore().Add("A", new TimeBufferedRecord(2));
        GetDataStore().Add("B", new TimeBufferedRecord(3));
        GetDataStore().Add("C", new TimeBufferedRecord(5));
    }

    @Override
    void OutputHandler() {
        while(true) {
            if (transactionSuccessCount+transactionFailCount>30000) continue;
            while (!ExecuteTransaction()) {
                transactionFailCount++;
                Log("Transaction failed! (" + transactionSuccessCount + "/" + (transactionSuccessCount + transactionFailCount) + ") (" + GetSuccessPercentString() + "%)");
            } //ripeto finchè non riesco
            transactionSuccessCount++;
            Log("Transaction completed! (" + transactionSuccessCount + "/" + (transactionSuccessCount + transactionFailCount) + ") (" + GetSuccessPercentString() + "%)");
        }
    }

    @Override
    boolean OnTransactionExecute(Timestamp curTimestamp,
                                 LinkedList<Payload> bufferedPrewrites,
                                 LinkedList<Payload> bufferedReads,
                                 LinkedList<Payload> bufferedWrites) {

        Payload plPrewriteB = new Payload();
        Payload plPrewriteC = new Payload();
        Payload plA = new Payload();
        Payload plB = new Payload();
        Payload plWriteB = new Payload();
        Payload plWriteC = new Payload();

        //Innanzitutto comunico i prewrite
        if (!PrewriteRequest(curTimestamp, GetRandomOutSocket(), "B", plPrewriteB, bufferedPrewrites)) return false;
        Debug(curTimestamp + " Prewrite B buffered on " + plPrewriteB.GetUsedSocket().getRemoteSocketAddress());

        if (!PrewriteRequest(curTimestamp, GetRandomOutSocket(), "C", plPrewriteC, bufferedPrewrites)) return false;
        Debug(curTimestamp + " Prewrite C buffered on " + plPrewriteC.GetUsedSocket().getRemoteSocketAddress());


        //Adesso i read, e se vengono bufferati li salvo a parte per recuperare i dati subito dopo
        if (!ReadRequest(curTimestamp, GetRandomOutSocket(), "A", plA, bufferedReads)) return false;
        Debug(curTimestamp + " Read A success/buffered on " + plA.GetUsedSocket().getRemoteSocketAddress());

        if (!ReadRequest(curTimestamp, GetRandomOutSocket(), "B", plB, bufferedReads)) return false;
        Debug(curTimestamp + " Read B success/buffered on " + plB.GetUsedSocket().getRemoteSocketAddress());


        //Smaltisco richieste READ bufferate
        WaitBuffer(bufferedReads, new Request[]{Request.SUCCESS_READ});


        if (!WriteRequest(curTimestamp, plPrewriteB.GetUsedSocket(), "B", plA.GetArg1()*plB.GetArg1(), plWriteB, bufferedWrites)) return false;
        Debug(curTimestamp + " Write B success/buffered on " + plWriteB.GetUsedSocket().getRemoteSocketAddress());

        if (!WriteRequest(curTimestamp, plPrewriteC.GetUsedSocket(), "C", plA.GetArg1()+plB.GetArg1(), plWriteC, bufferedWrites)) return false;
        Debug(curTimestamp + " Write C success/buffered on " + plWriteC.GetUsedSocket().getRemoteSocketAddress());


        //Smaltisco richieste WRITE bufferate
        WaitBuffer(bufferedWrites, new Request[]{Request.SUCCESS_WRITE});

        return true;
    }

    @Override
    void PayloadReceivedHandler(Payload payload) {
        ProcessRequest(payload,false);
    }

    void ProcessRequest(Payload payload, boolean buffered) {
        //Siccome qui arrivano le richieste sul listen socket, possiamo aspettarci solo read, write o prewrite
        //se buffered==true, vuol dire che stiamo verificando le richieste già presenti nel buffer e quindi non buffereremo alcuna richiesta
        String n = payload.GetTarget();
        try {
            GetDataStore().Lock(n);

            timestampLock.readLock().lock();
            Timestamp curTS = GetTimestamp();
            timestampLock.readLock().unlock();

            TimeBufferedRecord record = (TimeBufferedRecord) GetDataStore().GetValue(n);

            if (payload.GetRequest() == Request.READ) {
                Debug("Received READ request for " + n + " with timestamp " + payload.GetTimestamp()+ " from "+payload.GetUsedSocket().getRemoteSocketAddress());

                if (record.GetW_TS().IsGreaterThan(payload.GetTimestamp())) { //fallito per conflitto RW, abbiamo TS<W-TS
                    SendPayload(curTS, payload.GetUsedSocket(), Request.FAIL_READ, payload.GetTarget(), -1);
                    Debug("Rejected READ request for " + n + " with timestamp " + payload.GetTimestamp());
                } else if (payload.GetTimestamp().IsGreaterThan(record.GetMinP_TS())) { //TS>min-P-ts
                    if (!buffered) {
                        Debug("Buffered, min-P-ts=" + record.GetMinP_TS());
                        record.GetReadBuffer().add(payload);
                        SendPayload(curTS, payload.GetUsedSocket(), Request.BUFFERED_READ, payload.GetTarget(), -1);
                    }
                } else {
                    SendPayload(curTS, payload.GetUsedSocket(), Request.SUCCESS_READ, payload.GetTarget(), record.GetValue());
                    record.SetR_TS(payload.GetTimestamp());

                    record.GetReadBuffer().remove(payload); //se l'abbiamo letto dal buffer, va rimosso
                    Debug("Executed, min-R-ts=" + record.GetMinR_TS());
                    GetDataStore().Unlock(n);
                    if (!record.GetWriteBuffer().isEmpty()) ProcessRequest(record.GetWriteBuffer().peek(), true); //controlliamo se adesso è possibile soddisfare qualche write
                    if (!record.GetReadBuffer().isEmpty()) ProcessRequest(record.GetReadBuffer().peek(), true); //oppure altri read
                }

            } else if (payload.GetRequest() == Request.PREWRITE) {
                Debug("Received PREWRITE request for " + n + " with timestamp " + payload.GetTimestamp()+ " from "+payload.GetUsedSocket().getRemoteSocketAddress());

                if (record.GetW_TS().IsGreaterThan(payload.GetTimestamp()) || record.GetR_TS().IsGreaterThan(payload.GetTimestamp())) { //fallito per conflitto RW/WW, abbiamo TS<W-TS oppure TS<R-TS
                    SendPayload(curTS, payload.GetUsedSocket(), Request.FAIL_PREWRITE, payload.GetTarget(), -1);
                    Debug("Rejected PREWRITE request for " + n + " with timestamp " + payload.GetTimestamp());
                } else {
                    if (!buffered) {
                        SendPayload(curTS, payload.GetUsedSocket(), Request.BUFFERED_PREWRITE, payload.GetTarget(), payload.GetArg1());
                        record.GetPrewriteBuffer().add(payload);
                        Debug("Buffered, new min-P-ts=" + record.GetMinP_TS());
                    }
                }

            } else if (payload.GetRequest() == Request.WRITE) {
                Debug("Received WRITE request for " + n + " with timestamp " + payload.GetTimestamp()+ " from "+payload.GetUsedSocket().getRemoteSocketAddress());

                if (payload.GetTimestamp().IsGreaterThan(record.GetMinR_TS()) || payload.GetTimestamp().IsGreaterThan(record.GetMinP_TS())) { //write non viene mai rifiutato, ma se TS>min-R-ts o TS>min-P-ts lo mettiamo in buffer
                    if (!buffered) {
                        Debug("Buffered, min-R-ts=" + record.GetMinR_TS() + " and min-P-ts=" + record.GetMinP_TS());
                        SendPayload(curTS, payload.GetUsedSocket(), Request.BUFFERED_WRITE, payload.GetTarget(), -1);
                        record.GetWriteBuffer().add(payload);
                    }
                } else {
                    SendPayload(curTS, payload.GetUsedSocket(), Request.SUCCESS_WRITE, payload.GetTarget(), payload.GetArg1());
                    record.SetValue(payload.GetArg1());
                    record.SetW_TS(payload.GetTimestamp());
                    record.GetWriteBuffer().remove(payload); //se l'abbiamo letto dal buffer, va rimosso
                    record.GetPrewriteBuffer().removeIf((Payload p) -> (p.GetTarget().equals(payload.GetTarget()) && p.GetUsedSocket() == payload.GetUsedSocket())); //rimuovo prewrite associati con questo write
                    Debug("Executed, min-W-ts=" + record.GetMinW_TS() + ", min-P-ts=" + record.GetMinP_TS());
                    GetDataStore().Unlock(n);
                    if (!record.GetReadBuffer().isEmpty()) ProcessRequest(record.GetReadBuffer().peek(), true); //potremmo aver sbloccato qualche richiesta di read ora
                    if (!record.GetWriteBuffer().isEmpty()) ProcessRequest(record.GetWriteBuffer().peek(), true); //oppure di scrittura siccome dipendono da min-P-ts
                }

            } else if (payload.GetRequest() == Request.READ_CANCEL) {
                Debug("Received READ_CANCEL for "+ n + " with timestamp " + payload.GetTimestamp());
                record.GetReadBuffer().removeIf((Payload p)->(p.GetHost()==payload.GetHost() && payload.GetTimestamp().IsEqualTo(p.GetTimestamp())));
                SendPayload(curTS, payload.GetUsedSocket(), Request.SUCCESS_READ_CANCEL, payload.GetTarget(), -1);

                GetDataStore().Unlock(n);
                if (!record.GetReadBuffer().isEmpty()) ProcessRequest(record.GetReadBuffer().peek(), true); //potremmo aver sbloccato qualche richiesta di read ora
                if (!record.GetWriteBuffer().isEmpty()) ProcessRequest(record.GetWriteBuffer().peek(), true); //oppure di scrittura siccome dipendono da min-P-ts
            } else if (payload.GetRequest() == Request.WRITE_CANCEL) {
                Debug("Received WRITE_CANCEL for "+ n + " with timestamp " + payload.GetTimestamp());
                record.GetWriteBuffer().removeIf((Payload p)->(p.GetHost()==payload.GetHost() && payload.GetTimestamp().IsEqualTo(p.GetTimestamp())));
                SendPayload(curTS, payload.GetUsedSocket(), Request.SUCCESS_WRITE_CANCEL, payload.GetTarget(), -1);
            } else if (payload.GetRequest() == Request.PREWRITE_CANCEL) {
                Debug("Received PREWRITE_CANCEL for "+ n + " with timestamp " + payload.GetTimestamp());
                record.GetPrewriteBuffer().removeIf((Payload p)->(p.GetHost()==payload.GetHost() && payload.GetTimestamp().IsEqualTo(p.GetTimestamp())));
                SendPayload(curTS, payload.GetUsedSocket(), Request.SUCCESS_PREWRITE_CANCEL, payload.GetTarget(), -1);

                GetDataStore().Unlock(n);
                if (!record.GetReadBuffer().isEmpty()) ProcessRequest(record.GetReadBuffer().peek(), true); //potremmo aver sbloccato qualche richiesta di read ora
                if (!record.GetWriteBuffer().isEmpty()) ProcessRequest(record.GetWriteBuffer().peek(), true); //oppure di scrittura siccome dipendono da min-P-ts
            }
        } finally {
            GetDataStore().Unlock(n);
        }
    }
}
