package lamport.process;

import lamport.payload.TimestampedPayload;
import lamport.payload.TimestampedPayload;

import java.net.Socket;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ProcessPartialLamport extends Process<TimestampedPayload> {

    public ProcessPartialLamport(int port) {
        super(port);
    }

    private int timestamp=0;
    private Lock lockTimestamp=new ReentrantLock(); //l'accesso a timestamp deve essere gestito da un lock unico per evitare conflitti di concorrenza

    public int GetTimestamp() { return timestamp; }
    public void SetTimestamp(int t) { timestamp = t; }

    @Override
    void OutputSocketHandler(Socket s) {
        while(true) {
            try {
                Thread.sleep(ThreadLocalRandom.current().nextInt(100, 3000));
            } catch (Exception e) { }

            lockTimestamp.lock();

            SetTimestamp(GetTimestamp() + 1);

            TimestampedPayload payload = new TimestampedPayload();
            payload.SetTimestamp(GetTimestamp());

            Send(s, payload);

            lockTimestamp.unlock();
        }
    }

    @Override
    void PayloadReceivedHandler(Socket s, TimestampedPayload payload) {

        lockTimestamp.lock();

        int t=payload.GetTimestamp();
        int newT=Math.max(t,GetTimestamp())+1;
        Log("Received packet with timestamp "+t+", current timestamp is "+GetTimestamp()+".");
        Log("New timestamp is "+newT+".");
        SetTimestamp(newT);

        lockTimestamp.unlock();

    }

}