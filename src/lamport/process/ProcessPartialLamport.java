package lamport.process;

import lamport.payload.TimestampedPayload;
import lamport.timestamps.SimpleTimestamp;

import java.net.Socket;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ProcessPartialLamport extends Process<TimestampedPayload> {

    public ProcessPartialLamport(int port) {
        super(port);
    }

    private SimpleTimestamp timestamp=new SimpleTimestamp();
    private Lock lockTimestamp=new ReentrantLock(); //l'accesso a timestamp deve essere gestito da un lock unico per evitare conflitti di concorrenza

    public SimpleTimestamp GetTimestamp() { return timestamp; }

    @Override
    void OutputHandler() {
        while(true) {
            try {
                Thread.sleep(ThreadLocalRandom.current().nextInt(100, 3000));
            } catch (Exception e) { }

            lockTimestamp.lock();
            GetTimestamp().Add(1);
            lockTimestamp.unlock();

            TimestampedPayload payload = new TimestampedPayload();
            payload.GetTimestamp().Set(GetTimestamp());

            Send(GetRandomOutSocket(), payload);

        }
    }

    @Override
    void PayloadReceivedHandler(Socket s, TimestampedPayload payload) {

        lockTimestamp.lock();

        SimpleTimestamp t=payload.GetTimestamp();
        SimpleTimestamp newT=SimpleTimestamp.Max(t,GetTimestamp());
        newT.Add(1);

        Log("Received packet with timestamp "+t+", current timestamp is "+GetTimestamp()+".");
        GetTimestamp().Set(newT);
        Log("New timestamp is "+GetTimestamp()+".");

        lockTimestamp.unlock();

    }

}