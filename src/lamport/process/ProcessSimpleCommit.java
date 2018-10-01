package lamport.process;

import lamport.payload.Payload;
import lamport.payload.TimestampedIDPayload;
import lamport.timestamps.UniqueTimestamp;

import java.net.Socket;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ProcessSimpleCommit extends Process<TimestampedIDPayload> {


    public ProcessSimpleCommit(int port) {
        super(port);
        GetTimestamp().Set(0,port);
    }

    private UniqueTimestamp timestamp=new UniqueTimestamp(); //TODO: classe apposita per timestamp per evitare ridondanza
    private Lock lockTimestamp=new ReentrantLock(); //l'accesso a timestamp deve essere gestito da un lock unico per evitare conflitti di concorrenza

    public UniqueTimestamp GetTimestamp() { return timestamp; }

    TimestampedIDPayload Read(Socket s, String data, UniqueTimestamp transactionTS) {
        TimestampedIDPayload payload=new TimestampedIDPayload();
        payload.GetTimestamp().Set(transactionTS,false);
        payload.SetRequest(Payload.Request.READ);
        payload.SetTarget(data);

        lockTimestamp.lock();
        GetTimestamp().Add(1); //Il timestamp della transazione resterà quello per tutte le richieste della transazione, ma il timestamp del processo vogliamo che sia incrementato ad ogni messaggio comunque
        lockTimestamp.unlock();

        Send(s,payload);
        TimestampedIDPayload resPayload=Receive(s);

        return resPayload;
    }
    TimestampedIDPayload Write(Socket s, String data, int value, UniqueTimestamp transactionTS) {
        TimestampedIDPayload payload=new TimestampedIDPayload();
        payload.GetTimestamp().Set(transactionTS,false);
        payload.SetRequest(Payload.Request.WRITE); //è inteso write+commit in questo frangente
        payload.SetTarget(data);
        payload.SetArg1(value);

        lockTimestamp.lock();
        GetTimestamp().Add(1); //Il timestamp della transazione resterà quello per tutte le richieste della transazione, ma il timestamp del processo vogliamo che sia incrementato ad ogni messaggio comunque
        lockTimestamp.unlock();

        Send(s,payload);
        TimestampedIDPayload resPayload=Receive(s);

        return resPayload;
    }


    @Override
    void OutputHandler() {
        while(true) {
            try {
                Thread.sleep(ThreadLocalRandom.current().nextInt(100, 3000));
            } catch (Exception e) { }

            lockTimestamp.lock();

            UniqueTimestamp curTimestamp=GetTimestamp().clone();

            lockTimestamp.unlock();

            int A=Read(GetRandomOutSocket(),"A",curTimestamp).GetArg1();
            int B=Read(GetRandomOutSocket(),"B",curTimestamp).GetArg1();
            Write(GetRandomOutSocket(),"A",A+B,curTimestamp);

        }
    }

    @Override
    void PayloadReceivedHandler(Socket s, TimestampedIDPayload payload) {

        //Siccome qui arrivano le richieste sul listen socket, possiamo aspettarci solo read o write

        /*lockTimestamp.lock();

        int[] t=payload.GetTimestamp();
        int newT=Math.max(t[0],GetTimestamp()[0])+1;
        Log("Received packet with timestamp "+payload.GetTimestamp()+", current timestamp is "+GetTimestamp()+".");
        Log("New timestamp is "+GetTimestamp()+".");
        SetTimestamp(newT);

        lockTimestamp.unlock();*/

    }

}
