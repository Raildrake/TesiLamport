package lamport.process;

import lamport.datastore.MultiVersionBufferedRecord;
import lamport.payload.Payload;
import lamport.payload.Request;
import lamport.timestamps.Timestamp;

import java.util.LinkedList;

public class ProcessMV2PC extends TransactionalProcess {

    public ProcessMV2PC(int port) {
        super(port);
        GetTimestamp().Set(0,port);
    }
    @Override
    void InitData() {
        GetDataStore().Add("A", new MultiVersionBufferedRecord(2));
        GetDataStore().Add("B", new MultiVersionBufferedRecord(3));
        GetDataStore().Add("C", new MultiVersionBufferedRecord(5));
    }

    @Override
    void OutputHandler() {
        while(true) {
            GetStatCollector().StartAttempt();
            while (!ExecuteTransaction()) {
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

            MultiVersionBufferedRecord record = (MultiVersionBufferedRecord) GetDataStore().GetValue(n);

            if (payload.GetRequest() == Request.READ) {
                Debug("Received READ request for " + n + " with timestamp " + payload.GetTimestamp()+ " from "+payload.GetUsedSocket().getRemoteSocketAddress());

                if (record.FallsInPrewriteInterval(payload.GetTimestamp())) {
                    if (!buffered) {
                        Debug("Buffered READ for "+ n +".");
                        record.GetReadBuffer().add(payload);
                        SendPayload(curTS, payload.GetUsedSocket(), Request.BUFFERED_READ, payload.GetTarget(), -1);
                    }
                } else {
                    record.GetReadBuffer().remove(payload);
                    record.Read(payload.GetTimestamp());
                    SendPayload(curTS, payload.GetUsedSocket(), Request.SUCCESS_READ, payload.GetTarget(), record.GetOldestVersionBefore(payload.GetTimestamp()));

                    GetDataStore().Unlock(n);
                    if (!record.GetReadBuffer().isEmpty()) ProcessRequest(record.GetReadBuffer().peek(), true); //potremmo aver sbloccato qualche richiesta di read ora
                }

            } else if (payload.GetRequest() == Request.PREWRITE) {
                Debug("Received PREWRITE request for " + n + " with timestamp " + payload.GetTimestamp()+ " from "+payload.GetUsedSocket().getRemoteSocketAddress());

                Timestamp tEnd=record.GetFirstWriteAfter(payload.GetTimestamp());
                if (record.HasReadInInterval(payload.GetTimestamp(),tEnd)) {
                    SendPayload(curTS, payload.GetUsedSocket(), Request.FAIL_PREWRITE, payload.GetTarget(), -1);
                } else {
                    if (!buffered) {
                        Debug("Buffered PREWRITE for "+ n +".");
                        record.GetPrewriteBuffer().add(payload);
                        SendPayload(curTS, payload.GetUsedSocket(), Request.BUFFERED_PREWRITE, payload.GetTarget(), -1);
                    }
                }

            } else if (payload.GetRequest() == Request.WRITE) {
                Debug("Received WRITE request for " + n + " with timestamp " + payload.GetTimestamp()+ " from "+payload.GetUsedSocket().getRemoteSocketAddress());

                record.Write(payload.GetArg1(),payload.GetTimestamp());
                record.GetPrewriteBuffer().removeIf((Payload p) -> (p.GetTarget().equals(payload.GetTarget()) && p.GetUsedSocket() == payload.GetUsedSocket())); //rimuovo prewrite associati con questo write
                SendPayload(curTS, payload.GetUsedSocket(), Request.SUCCESS_WRITE, payload.GetTarget(), -1);

                GetDataStore().Unlock(n);
                if (!record.GetReadBuffer().isEmpty()) ProcessRequest(record.GetReadBuffer().peek(), true);

            }
            else if (payload.GetRequest() == Request.READ_CANCEL) {
                Debug("Received READ_CANCEL for "+ n + " with timestamp " + payload.GetTimestamp());
                record.GetReadBuffer().removeIf((Payload p)->(p.GetHost()==payload.GetHost() && payload.GetTimestamp().IsEqualTo(p.GetTimestamp())));
                SendPayload(curTS, payload.GetUsedSocket(), Request.SUCCESS_READ_CANCEL, payload.GetTarget(), -1);

                GetDataStore().Unlock(n);
                if (!record.GetReadBuffer().isEmpty()) ProcessRequest(record.GetReadBuffer().peek(), true);

            } else if (payload.GetRequest() == Request.PREWRITE_CANCEL) {
                Debug("Received PREWRITE_CANCEL for "+ n + " with timestamp " + payload.GetTimestamp());
                record.GetPrewriteBuffer().removeIf((Payload p)->(p.GetHost()==payload.GetHost() && payload.GetTimestamp().IsEqualTo(p.GetTimestamp())));
                SendPayload(curTS, payload.GetUsedSocket(), Request.SUCCESS_PREWRITE_CANCEL, payload.GetTarget(), -1);

                GetDataStore().Unlock(n);
                if (!record.GetReadBuffer().isEmpty()) ProcessRequest(record.GetReadBuffer().peek(), true);

            }
        } finally {
            GetDataStore().Unlock(n);
        }
    }

}
