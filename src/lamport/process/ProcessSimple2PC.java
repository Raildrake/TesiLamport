package lamport.process;

import javafx.util.Pair;
import lamport.datastore.TimeBufferedRecord;
import lamport.datastore.TimestampedRecord;
import lamport.payload.Payload;
import lamport.payload.Request;
import lamport.payload.TimestampedIDPayload;
import lamport.timestamps.UniqueTimestamp;

import java.net.Socket;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ProcessSimple2PC extends ProcessSimple1PC {

    public ProcessSimple2PC(int port) {
        super(port);

        GetDataStore().Clear(); //ereditiamo da 1pc ma vogliamo cambiare il tipo di dati, da migliorare in seguito
        GetDataStore().Add("A",new TimeBufferedRecord(2));
        GetDataStore().Add("B",new TimeBufferedRecord(3));
        GetDataStore().Add("C",new TimeBufferedRecord(5));
    }

    @Override
    void OutputHandler() {
        while(true) {
            LinkedList<TimestampedIDPayload> bufferedReads=new LinkedList<>();
            LinkedList<TimestampedIDPayload> bufferedWrites=new LinkedList<>();
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

        LinkedList<TimestampedIDPayload> bufferedReads=new LinkedList<>();
        LinkedList<TimestampedIDPayload> bufferedWrites=new LinkedList<>();
        LinkedList<TimestampedIDPayload> bufferedPrewrites=new LinkedList<>();

        try {

            TimestampedIDPayload plPrewriteB = new TimestampedIDPayload();
            TimestampedIDPayload plPrewriteC = new TimestampedIDPayload();
            TimestampedIDPayload plA = new TimestampedIDPayload();
            TimestampedIDPayload plB = new TimestampedIDPayload();
            TimestampedIDPayload plWriteB = new TimestampedIDPayload();
            TimestampedIDPayload plWriteC = new TimestampedIDPayload();

            //Innanzitutto comunico i prewrite
            if (!PrewriteRequest(curTimestamp, GetRandomOutSocket(), "B", plPrewriteB, bufferedPrewrites)) return false;
            Debug(curTimestamp + " Prewrite B buffered on "+plPrewriteB.GetUsedSocket().getRemoteSocketAddress());

            if (!PrewriteRequest(curTimestamp, GetRandomOutSocket(), "C", plPrewriteC, bufferedPrewrites)) return false;
            Debug(curTimestamp + " Prewrite C buffered on "+plPrewriteC.GetUsedSocket().getRemoteSocketAddress());


            //Adesso i read, e se vengono bufferati li salvo a parte per recuperare i dati subito dopo
            if (!ReadRequest(curTimestamp,GetRandomOutSocket(),"A",plA,bufferedReads)) return false;
            Debug(curTimestamp + " Read A success/buffered on "+plA.GetUsedSocket().getRemoteSocketAddress());

            if (!ReadRequest(curTimestamp,GetRandomOutSocket(),"B",plB,bufferedReads)) return false;
            Debug(curTimestamp + " Read B success/buffered on "+plB.GetUsedSocket().getRemoteSocketAddress());


            //Smaltisco richieste READ bufferate
            WaitBuffer(bufferedReads,new Request[]{Request.SUCCESS_READ});


            if (!WriteRequest(curTimestamp,plPrewriteB.GetUsedSocket(),"B",0,plWriteB,bufferedWrites)) return false;
            Debug(curTimestamp + " Write B success/buffered on "+plWriteB.GetUsedSocket().getRemoteSocketAddress());

            if (!WriteRequest(curTimestamp,plPrewriteC.GetUsedSocket(),"C",0,plWriteC,bufferedWrites)) return false;
            Debug(curTimestamp + " Write C success/buffered on "+plWriteC.GetUsedSocket().getRemoteSocketAddress());

            
            //Smaltisco richieste WRITE bufferate
            WaitBuffer(bufferedWrites,new Request[]{Request.SUCCESS_WRITE});
            //Se sono arrivato qui la transazione è andata bene e non ho sicuramente prewrite da pulire
            bufferedPrewrites.clear();
            return true;

        } finally {
            //potremmo aver fallito e alcuni buffer potrebbero essere rimasti contaminati, quindi li puliamo
            while (bufferedWrites.size()>0) {
                TimestampedIDPayload buffered=bufferedWrites.get(0);
                WriteCancelRequest(curTimestamp,buffered.GetUsedSocket(),buffered.GetTarget());
                bufferedWrites.removeFirst();
            }
            while (bufferedReads.size()>0) {
                TimestampedIDPayload buffered=bufferedReads.get(0);
                ReadCancelRequest(curTimestamp,buffered.GetUsedSocket(),buffered.GetTarget());
                bufferedReads.removeFirst();
            }
            while (bufferedPrewrites.size()>0) {
                TimestampedIDPayload buffered=bufferedPrewrites.get(0);
                PrewriteCancelRequest(curTimestamp,buffered.GetUsedSocket(),buffered.GetTarget());
                bufferedPrewrites.removeFirst();
            }
        }
    }

    @Override
    void PayloadReceivedHandler(TimestampedIDPayload payload) {
        ProcessRequest(payload,false);
    }

    void ProcessRequest(TimestampedIDPayload payload, boolean buffered) {
        //Siccome qui arrivano le richieste sul listen socket, possiamo aspettarci solo read, write o prewrite
        //se buffered==true, vuol dire che stiamo verificando le richieste già presenti nel buffer e quindi non buffereremo alcuna richiesta
        String n = payload.GetTarget();
        try {
            GetDataStore().Lock(n);

            timestampLock.readLock().lock();
            UniqueTimestamp curTS = GetTimestamp();
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
                    record.GetPrewriteBuffer().removeIf((TimestampedIDPayload p) -> (p.GetTarget().equals(payload.GetTarget()) && p.GetUsedSocket() == payload.GetUsedSocket())); //rimuovo prewrite associati con questo write
                    Debug("Executed, min-W-ts=" + record.GetMinW_TS() + ", min-P-ts=" + record.GetMinP_TS());
                    GetDataStore().Unlock(n);
                    if (!record.GetReadBuffer().isEmpty()) ProcessRequest(record.GetReadBuffer().peek(), true); //potremmo aver sbloccato qualche richiesta di read ora
                    if (!record.GetWriteBuffer().isEmpty()) ProcessRequest(record.GetWriteBuffer().peek(), true); //oppure di scrittura siccome dipendono da min-P-ts
                }

            } else if (payload.GetRequest() == Request.READ_CANCEL) {
                Debug("Received READ_CANCEL for "+ n + " with timestamp " + payload.GetTimestamp());
                record.GetReadBuffer().removeIf((TimestampedIDPayload p)->(p.GetHost()==payload.GetHost() && payload.GetTimestamp().IsEqualTo(p.GetTimestamp())));
                SendPayload(curTS, payload.GetUsedSocket(), Request.SUCCESS_READ_CANCEL, payload.GetTarget(), -1);

                GetDataStore().Unlock(n);
                if (!record.GetReadBuffer().isEmpty()) ProcessRequest(record.GetReadBuffer().peek(), true); //potremmo aver sbloccato qualche richiesta di read ora
                if (!record.GetWriteBuffer().isEmpty()) ProcessRequest(record.GetWriteBuffer().peek(), true); //oppure di scrittura siccome dipendono da min-P-ts
            } else if (payload.GetRequest() == Request.WRITE_CANCEL) {
                Debug("Received WRITE_CANCEL for "+ n + " with timestamp " + payload.GetTimestamp());
                record.GetWriteBuffer().removeIf((TimestampedIDPayload p)->(p.GetHost()==payload.GetHost() && payload.GetTimestamp().IsEqualTo(p.GetTimestamp())));
                SendPayload(curTS, payload.GetUsedSocket(), Request.SUCCESS_WRITE_CANCEL, payload.GetTarget(), -1);
            } else if (payload.GetRequest() == Request.PREWRITE_CANCEL) {
                Debug("Received PREWRITE_CANCEL for "+ n + " with timestamp " + payload.GetTimestamp());
                record.GetPrewriteBuffer().removeIf((TimestampedIDPayload p)->(p.GetHost()==payload.GetHost() && payload.GetTimestamp().IsEqualTo(p.GetTimestamp())));
                SendPayload(curTS, payload.GetUsedSocket(), Request.SUCCESS_PREWRITE_CANCEL, payload.GetTarget(), -1);

                GetDataStore().Unlock(n);
                if (!record.GetReadBuffer().isEmpty()) ProcessRequest(record.GetReadBuffer().peek(), true); //potremmo aver sbloccato qualche richiesta di read ora
                if (!record.GetWriteBuffer().isEmpty()) ProcessRequest(record.GetWriteBuffer().peek(), true); //oppure di scrittura siccome dipendono da min-P-ts
            }
        } finally {
            GetDataStore().Unlock(n);
        }
    }

    @Override
    protected String CurTime() {
        return GetTimestamp().toString();
    }
}
