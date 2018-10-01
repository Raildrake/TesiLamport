package lamport.process;

import lamport.payload.TimestampedPayload;
import lamport.payload.TimestampedPayload;
import lamport.timestamps.SimpleTimestamp;

import java.net.Socket;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ProcessNoSync extends Process<TimestampedPayload> {

    public ProcessNoSync(int port) {
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

            synchronized(GetTimestamp()) {
                GetTimestamp().Add(1);
            }

            TimestampedPayload payload = new TimestampedPayload();
            payload.GetTimestamp().Set(GetTimestamp());

            Send(GetRandomOutSocket(), payload);

        }
    }

    @Override
    void PayloadReceivedHandler(Socket s, TimestampedPayload payload) {

        lockTimestamp.lock();

        SimpleTimestamp t=payload.GetTimestamp();
        Log("Received packet with timestamp "+t+", current timestamp is "+GetTimestamp()+".");

        lockTimestamp.unlock();

    }

}