package lamport.process;

import javafx.util.Pair;
import lamport.datastore.TimeBufferedRecord;
import lamport.datastore.TimestampedRecord;
import lamport.payload.Payload;
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

    public UniqueTimestamp GetTimestamp() { return timestamp; }

    @Override
    void OutputHandler() {
        while(true) {
            try {
                Thread.sleep(ThreadLocalRandom.current().nextInt(100, 3000));
            } catch (Exception e) { }

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

            //Innanzitutto comunico i prewrite
            TimestampedIDPayload plPrewriteB = SendAndWaitResponse(curTimestamp, GetRandomOutSocket(), Payload.Request.PREWRITE, "B", -1);
            if (plPrewriteB.GetRequest() == Payload.Request.FAIL) return false;
            bufferedPrewrites.add(plPrewriteB);
            Log(curTimestamp + " Prewrite B buffered!");
            TimestampedIDPayload plPrewriteC = SendAndWaitResponse(curTimestamp, GetRandomOutSocket(), Payload.Request.PREWRITE, "C", -1);
            if (plPrewriteC.GetRequest() == Payload.Request.FAIL) return false;
            bufferedPrewrites.add(plPrewriteC);
            Log(curTimestamp + " Prewrite C buffered!");

            //Adesso i read, e se vengono bufferati li salvo a parte per recuperare i dati subito dopo
            TimestampedIDPayload plA = SendAndWaitResponse(curTimestamp, GetRandomOutSocket(), Payload.Request.READ, "A", -1);
            if (plA.GetRequest() == Payload.Request.FAIL) return false;
            if (plA.GetRequest() == Payload.Request.BUFFERED_READ) {
                bufferedReads.add(plA);
                Log(curTimestamp + " Read A buffered!");
            } else Log(curTimestamp + " Read A success!");
            TimestampedIDPayload plB = SendAndWaitResponse(curTimestamp, GetRandomOutSocket(), Payload.Request.READ, "B", -1);
            if (plB.GetRequest() == Payload.Request.FAIL) return false;
            if (plB.GetRequest() == Payload.Request.BUFFERED_READ) {
                bufferedReads.add(plB);
                Log(curTimestamp + " Read B buffered!");
            } else Log(curTimestamp + " Read B success!");

            while (bufferedReads.size() > 0) {
                TimestampedIDPayload received = Receive(bufferedReads.get(0).GetUsedSocket());
                if (received == null) continue;
                ProcessTimestamp(received);
                Log(curTimestamp + " Read " + received.GetTarget() + " success!");
                bufferedReads.removeFirst();
            }

            TimestampedIDPayload plWriteB = SendAndWaitResponse(curTimestamp, plPrewriteB.GetUsedSocket(), Payload.Request.WRITE, "B", plA.GetArg1() + plB.GetArg1());
            if (plWriteB.GetRequest() == Payload.Request.BUFFERED_WRITE) {
                bufferedWrites.add(plWriteB);
                Log(curTimestamp + " Write B buffered!");
            } else Log(curTimestamp + " Write B success!");
            TimestampedIDPayload plWriteC = SendAndWaitResponse(curTimestamp, plPrewriteC.GetUsedSocket(), Payload.Request.WRITE, "C", plA.GetArg1() - plB.GetArg1());
            if (plWriteC.GetRequest() == Payload.Request.BUFFERED_WRITE) {
                bufferedWrites.add(plWriteC);
                Log(curTimestamp + " Write C buffered!");
            } else Log(curTimestamp + " Write C success!");

            while (bufferedWrites.size() > 0) {
                TimestampedIDPayload received = Receive(bufferedWrites.get(0).GetUsedSocket());
                if (received == null) continue;
                ProcessTimestamp(received);
                Log(curTimestamp + " Write " + received.GetTarget() + " success!");
                bufferedWrites.removeFirst();
            }
            return true;

        } finally {
            //potremmo aver fallito e alcuni buffer potrebbero essere rimasti contaminati, quindi li puliamo
            while (bufferedReads.size()>0) {
                TimestampedIDPayload r=bufferedReads.get(0);
                SendAndWaitResponse(curTimestamp,r.GetUsedSocket(),Payload.Request.READ_CANCEL,r.GetTarget(),-1);
                bufferedReads.removeFirst();
            }
            while (bufferedWrites.size()>0) {
                TimestampedIDPayload r=bufferedWrites.get(0);
                SendAndWaitResponse(curTimestamp,r.GetUsedSocket(),Payload.Request.WRITE_CANCEL,r.GetTarget(),-1);
                bufferedWrites.removeFirst();
            }
            while (bufferedPrewrites.size()>0) {
                TimestampedIDPayload r=bufferedPrewrites.get(0);
                SendAndWaitResponse(curTimestamp,r.GetUsedSocket(),Payload.Request.PREWRITE_CANCEL,r.GetTarget(),-1);
                bufferedPrewrites.removeFirst();
            }
        }
    }

    @Override
    void PayloadReceivedHandler(Socket s, TimestampedIDPayload payload) {

        //elaboro prima il timestamp a prescindere
        ProcessTimestamp(payload);

        //ora elaboro la richiesta
        ProcessRequest(s,payload,false);
    }

    void ProcessRequest(Socket s, TimestampedIDPayload payload, boolean buffered) {
        //Siccome qui arrivano le richieste sul listen socket, possiamo aspettarci solo read, write o prewrite
        //se buffered==true, vuol dire che stiamo verificando le richieste già presenti nel buffer
        String n = payload.GetTarget();
        try {
            GetDataStore().Lock(n);

            timestampLock.readLock().lock();
            UniqueTimestamp ts = GetTimestamp();
            timestampLock.readLock().unlock();

            TimeBufferedRecord record = (TimeBufferedRecord) GetDataStore().GetValue(n);

            if (payload.GetRequest() == Payload.Request.READ) {
                Log("Received READ request for " + n + " with timestamp " + payload.GetTimestamp());

                if (record.GetW_TS().IsGreaterThan(payload.GetTimestamp())) { //fallito per conflitto RW, abbiamo TS<W-TS
                    SendPayload(ts, s, Payload.Request.FAIL, payload.GetTarget(), -1);
                } else if (payload.GetTimestamp().IsGreaterThan(record.GetMinP_TS())) { //TS>min-P-ts
                    if (!buffered) {
                        Log("Buffered, min-P-ts=" + record.GetMinP_TS());
                        record.GetReadBuffer().add(payload);
                        SendPayload(ts, s, Payload.Request.BUFFERED_READ, payload.GetTarget(), -1);
                    }
                } else {
                    SendPayload(ts, s, Payload.Request.SUCCESS_READ, payload.GetTarget(), record.GetValue());
                    record.SetR_TS(payload.GetTimestamp());

                    if (buffered) record.GetReadBuffer().poll(); //se l'abbiamo letto dal buffer, va rimosso
                    Log("Executed, min-R-ts=" + record.GetMinR_TS());
                    if (!record.GetWriteBuffer().isEmpty())
                        ProcessRequest(record.GetWriteBuffer().peek().GetUsedSocket(), record.GetWriteBuffer().peek(), true); //controlliamo se adesso è possibile soddisfare qualche write
                    if (!record.GetReadBuffer().isEmpty())
                        ProcessRequest(record.GetReadBuffer().peek().GetUsedSocket(), record.GetReadBuffer().peek(), true); //oppure altri read
                }

            } else if (payload.GetRequest() == Payload.Request.PREWRITE) {
                Log("Received PREWRITE request for " + n + " with timestamp " + payload.GetTimestamp());

                if (record.GetW_TS().IsGreaterThan(payload.GetTimestamp()) || record.GetR_TS().IsGreaterThan(payload.GetTimestamp())) { //fallito per conflitto RW/WW, abbiamo TS<W-TS oppure TS<R-TS
                    SendPayload(ts, s, Payload.Request.FAIL, payload.GetTarget(), -1);
                } else {
                    if (!buffered) {
                        SendPayload(ts, s, Payload.Request.BUFFERED_PREWRITE, payload.GetTarget(), payload.GetArg1());
                        record.GetPrewriteBuffer().add(payload);
                        Log("Buffered, new min-P-ts=" + record.GetMinP_TS());
                    }
                }

            } else if (payload.GetRequest() == Payload.Request.WRITE) {
                Log("Received WRITE request for " + n + " with timestamp " + payload.GetTimestamp());

                if (payload.GetTimestamp().IsGreaterThan(record.GetMinR_TS()) || payload.GetTimestamp().IsGreaterThan(record.GetMinP_TS())) { //write non viene mai rifiutato, ma se TS>min-R-ts o TS>min-P-ts lo mettiamo in buffer
                    if (!buffered) {
                        Log("Buffered, min-R-ts=" + record.GetMinR_TS() + " and min-P-ts=" + record.GetMinP_TS());
                        SendPayload(ts, s, Payload.Request.BUFFERED_WRITE, payload.GetTarget(), -1);
                        record.GetWriteBuffer().add(payload);
                    }
                } else {
                    SendPayload(ts, s, Payload.Request.SUCCESS_WRITE, payload.GetTarget(), payload.GetArg1());
                    record.SetValue(payload.GetArg1());
                    record.SetW_TS(payload.GetTimestamp());
                    //Log("Prewrite buffer before: "+prewriteBuffer.size()+" min-P-ts="+GetMinP_TS());
                    if (buffered) record.GetWriteBuffer().poll(); //se l'abbiamo letto dal buffer, va rimosso
                    //Log("Remove from prewrite where "+payload.GetTarget()+" and "+payload.GetHost());
                    record.GetPrewriteBuffer().removeIf((TimestampedIDPayload p) -> (p.GetTarget().equals(payload.GetTarget()) && p.GetHost() == payload.GetHost())); //rimuovo prewrite associati con questo write
                    Log("Executed, min-W-ts=" + record.GetMinW_TS() + ", min-P-ts=" + record.GetMinP_TS());
                    //Log("Prewrite buffer after: "+prewriteBuffer.size()+" min-P-ts="+GetMinP_TS());
                    if (!record.GetReadBuffer().isEmpty())
                        ProcessRequest(record.GetReadBuffer().peek().GetUsedSocket(), record.GetReadBuffer().peek(), true); //potremmo aver sbloccato qualche richiesta di read ora
                    if (!record.GetWriteBuffer().isEmpty())
                        ProcessRequest(record.GetWriteBuffer().peek().GetUsedSocket(), record.GetWriteBuffer().peek(), true); //oppure di scrittura siccome dipendono da min-P-ts
                }

            } else if (payload.GetRequest() == Payload.Request.READ_CANCEL) {
                record.GetReadBuffer().removeIf((TimestampedIDPayload p)->(p.GetHost()==payload.GetHost() && payload.GetTimestamp().IsEqualTo(p.GetTimestamp())));
                SendPayload(ts, s, Payload.Request.SUCCESS_CANCEL, payload.GetTarget(), -1);
            } else if (payload.GetRequest() == Payload.Request.WRITE_CANCEL) {
                record.GetWriteBuffer().removeIf((TimestampedIDPayload p)->(p.GetHost()==payload.GetHost() && payload.GetTimestamp().IsEqualTo(p.GetTimestamp())));
                SendPayload(ts, s, Payload.Request.SUCCESS_CANCEL, payload.GetTarget(), -1);
            } else if (payload.GetRequest() == Payload.Request.PREWRITE_CANCEL) {
                record.GetPrewriteBuffer().removeIf((TimestampedIDPayload p)->(p.GetHost()==payload.GetHost() && payload.GetTimestamp().IsEqualTo(p.GetTimestamp())));
                SendPayload(ts, s, Payload.Request.SUCCESS_CANCEL, payload.GetTarget(), -1);
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
