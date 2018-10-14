package lamport.process;

import lamport.datastore.DataStore;
import lamport.datastore.TimeBufferedRecord;
import lamport.payload.Payload;
import lamport.payload.Request;
import lamport.timestamps.Timestamp;

import java.net.Socket;
import java.util.LinkedList;

public class ProcessConservative extends TransactionalProcess {

    private DataStore processBuffers=new DataStore();
    private Thread keepAliveThread;

    public ProcessConservative(int port) {
        super(port);
        GetTimestamp().Set(0,port);

        keepAliveThread=new Thread(() -> {
            while(true) {

                timestampLock.readLock().lock();
                Timestamp curTS = GetTimestamp();
                timestampLock.readLock().unlock();

                for (Socket s : outSockets) {
                    SendPayload(curTS,s,Request.READ,"KEEPALIVE",-1);
                    SendPayload(curTS,s,Request.WRITE,"KEEPALIVE",-1);
                }

                try {
                    Thread.sleep(500);
                } catch (Exception e) {}
            }
        });
        keepAliveThread.start();
    }
    @Override
    void InitData() {
        GetDataStore().Add("A", new TimeBufferedRecord(2));
        GetDataStore().Add("B", new TimeBufferedRecord(3));
        GetDataStore().Add("C", new TimeBufferedRecord(5));
        GetDataStore().Add("KEEPALIVE", new TimeBufferedRecord(5));
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


        if (!WriteRequest(curTimestamp, GetRandomOutSocket(), "B", plA.GetArg1()*plB.GetArg1(), plWriteB, bufferedWrites)) return false;
        Debug(curTimestamp + " Write B success/buffered on " + plWriteB.GetUsedSocket().getRemoteSocketAddress());

        if (!WriteRequest(curTimestamp, GetRandomOutSocket(), "C", plA.GetArg1()+plB.GetArg1(), plWriteC, bufferedWrites)) return false;
        Debug(curTimestamp + " Write C success/buffered on " + plWriteC.GetUsedSocket().getRemoteSocketAddress());


        //Smaltisco richieste WRITE bufferate
        WaitBuffer(bufferedWrites, new Request[]{Request.SUCCESS_WRITE});

        return true;
    }

    String BufGetName(Payload p) {
        return p.GetUsedSocket().getRemoteSocketAddress().toString();
    }
    void BufVerify(Payload p) {
        if  (!processBuffers.Exists(BufGetName(p))) {
            processBuffers.Add(BufGetName(p),new TimeBufferedRecord(-1));
        }
    }
    synchronized void BufAddW(Payload p) {
        TimeBufferedRecord tbr=(TimeBufferedRecord)processBuffers.GetValue(BufGetName(p));
        tbr.GetWriteBuffer().removeIf(r->r.GetTarget().equals("KEEPALIVE"));
        tbr.GetWriteBuffer().add(p);
    }
    synchronized void BufAddR(Payload p) {
        TimeBufferedRecord tbr=(TimeBufferedRecord)processBuffers.GetValue(BufGetName(p));
        tbr.GetReadBuffer().removeIf(r->r.GetTarget().equals("KEEPALIVE"));
        tbr.GetReadBuffer().add(p);
    }
    synchronized void BufRemoveW(Payload p) {
        TimeBufferedRecord tbr=(TimeBufferedRecord)processBuffers.GetValue(BufGetName(p));
        tbr.GetWriteBuffer().remove(p);
    }
    synchronized void BufRemoveR(Payload p) {
        TimeBufferedRecord tbr=(TimeBufferedRecord)processBuffers.GetValue(BufGetName(p));
        tbr.GetReadBuffer().remove(p);
    }
    synchronized Timestamp BufMinW_TS() {
        Timestamp res=Timestamp.MAX_VALUE;

        for (String n : processBuffers.GetKeys()) {
            TimeBufferedRecord tbr=(TimeBufferedRecord)processBuffers.GetValue(n);
            if (tbr.GetWriteBuffer().size()==0) return Timestamp.MIN_VALUE;
            Timestamp min=tbr.GetMinW_TS();
            if (res.IsGreaterThan(min)) res=min.clone();
        }
        return res;
    }
    synchronized Timestamp BufMinR_TS() {
        Timestamp res=Timestamp.MAX_VALUE;

        for (String n : processBuffers.GetKeys()) {
            TimeBufferedRecord tbr=(TimeBufferedRecord)processBuffers.GetValue(n);
            if (tbr.GetReadBuffer().size()==0) return Timestamp.MIN_VALUE;
            Timestamp min=tbr.GetMinR_TS();
            if (res.IsGreaterThan(min)) res=min.clone();
        }

        return res;
    }

    @Override
    void PayloadReceivedHandler(Payload payload) {

        ProcessRequest(payload,false);

    }

    synchronized void ProcessRequest(Payload payload, boolean buffered) {
        //Siccome qui arrivano le richieste sul listen socket, possiamo aspettarci solo read, write o prewrite
        //se buffered==true, vuol dire che stiamo verificando le richieste già presenti nel buffer e quindi non buffereremo alcuna richiesta
        String n = payload.GetTarget();

        BufVerify(payload);

        try {
            GetDataStore().Lock(n);

            timestampLock.readLock().lock();
            Timestamp curTS = GetTimestamp();
            timestampLock.readLock().unlock();

            TimeBufferedRecord record = (TimeBufferedRecord) GetDataStore().GetValue(n);

            Debug("MINR: "+BufMinR_TS()+ ", MINW: "+BufMinW_TS()+ ", "+buffered);

            if (payload.GetRequest() == Request.READ) {
                Debug("Received READ request for " + n + " with timestamp " + payload.GetTimestamp() + " from " + payload.GetUsedSocket().getRemoteSocketAddress());

                if (payload.GetTimestamp().IsGreaterThan(BufMinR_TS())) {
                    if (!buffered) {
                        BufAddR(payload);
                        SendPayload(curTS, payload.GetUsedSocket(), Request.BUFFERED_READ, payload.GetTarget(), -1);

                        GetDataStore().Unlock(n);
                        TestBuffers();
                    }
                } else {
                    SendPayload(curTS, payload.GetUsedSocket(), Request.SUCCESS_READ, payload.GetTarget(), record.GetValue());
                    record.SetR_TS(payload.GetTimestamp());
                    BufRemoveR(payload);

                    GetDataStore().Unlock(n);
                    TestBuffers();
                }

            } else if (payload.GetRequest() == Request.PREWRITE) {
                Debug("Received PREWRITE request for " + n + " with timestamp " + payload.GetTimestamp() + " from " + payload.GetUsedSocket().getRemoteSocketAddress());

                //DUMMY
                SendPayload(curTS, payload.GetUsedSocket(), Request.BUFFERED_PREWRITE, payload.GetTarget(), -1);

            } else if (payload.GetRequest() == Request.WRITE) {
                Debug("Received WRITE request for " + n + " with timestamp " + payload.GetTimestamp() + " from " + payload.GetUsedSocket().getRemoteSocketAddress());

                if (payload.GetTimestamp().IsGreaterThan(BufMinW_TS()) || payload.GetTimestamp().IsGreaterThan(BufMinR_TS())) {
                    if (!buffered) {
                        BufAddW(payload);
                        SendPayload(curTS, payload.GetUsedSocket(), Request.BUFFERED_WRITE, payload.GetTarget(), -1);

                        GetDataStore().Unlock(n);
                        TestBuffers();
                    }
                } else {
                    record.SetValue(payload.GetArg1());
                    record.SetW_TS(payload.GetTimestamp());
                    BufRemoveW(payload);
                    SendPayload(curTS, payload.GetUsedSocket(), Request.SUCCESS_WRITE, payload.GetTarget(), payload.GetArg1());

                    GetDataStore().Unlock(n);
                    TestBuffers();
                }

            } else if (payload.GetRequest() == Request.READ_CANCEL) {
            } else if (payload.GetRequest() == Request.WRITE_CANCEL) {
            } else if (payload.GetRequest() == Request.PREWRITE_CANCEL) {
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            GetDataStore().Unlock(n);
        }
    }
    synchronized void TestBuffers() {
        Timestamp minW=BufMinW_TS();
        Timestamp minR=BufMinR_TS();
        for (String k : processBuffers.GetKeys()) {
            TimeBufferedRecord tbr=(TimeBufferedRecord)processBuffers.GetValue(k);
            if (!tbr.GetWriteBuffer().isEmpty() && tbr.GetWriteBuffer().peek().GetTimestamp().IsEqualTo(minW)) ProcessRequest(tbr.GetWriteBuffer().peek(), true);
            if (!tbr.GetReadBuffer().isEmpty() && tbr.GetReadBuffer().peek().GetTimestamp().IsEqualTo(minR)) ProcessRequest(tbr.GetReadBuffer().peek(), true);
        }
    }

}
