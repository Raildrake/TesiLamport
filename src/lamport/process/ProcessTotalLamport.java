package lamport.process;

import lamport.payload.TimestampedIDPayload;
import lamport.payload.TimestampedPayload;
import lamport.payload.TimestampedPayload;
import lamport.timestamps.UniqueTimestamp;

import java.net.Socket;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ProcessTotalLamport extends Process<TimestampedIDPayload> {

    public ProcessTotalLamport(int port) {
        super(port);
        GetTimestamp().Set(0,port);
    }

    private UniqueTimestamp timestamp=new UniqueTimestamp(); //TODO: classe apposita per timestamp per evitare ridondanza
    private Lock lockTimestamp=new ReentrantLock(); //l'accesso a timestamp deve essere gestito da un lock unico per evitare conflitti di concorrenza

    public UniqueTimestamp GetTimestamp() { return timestamp; }

    @Override
    void OutputHandler() {
        while(true) {
            try {
                Thread.sleep(ThreadLocalRandom.current().nextInt(100, 3000));
            } catch (Exception e) { }

            lockTimestamp.lock();
            GetTimestamp().Add(1);
            lockTimestamp.unlock();

            TimestampedIDPayload payload = new TimestampedIDPayload();
            payload.GetTimestamp().Set(GetTimestamp(),false);

            Send(GetRandomOutSocket(), payload);

        }
    }

    @Override
    void PayloadReceivedHandler(Socket s, TimestampedIDPayload payload) {

        lockTimestamp.lock();

        UniqueTimestamp t=payload.GetTimestamp();
        UniqueTimestamp newT=UniqueTimestamp.Max(t,GetTimestamp());
        newT.Add(1);
        Log("Received packet with timestamp "+payload.GetTimestamp()+", current timestamp is "+GetTimestamp()+".");
        GetTimestamp().Set(newT,true);
        Log("New timestamp is "+GetTimestamp()+".");

        lockTimestamp.unlock();

    }

}