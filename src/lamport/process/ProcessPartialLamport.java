package lamport.process;

import lamport.payload.TimestampedPayload;
import lamport.timestamps.SimpleTimestamp;

import java.net.Socket;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ProcessPartialLamport extends Process<TimestampedPayload, SimpleTimestamp> {

    public ProcessPartialLamport(int port) {
        super(port,SimpleTimestamp.class);
    }

    private ReadWriteLock timestampLock=new ReentrantReadWriteLock();

    @Override
    void OutputHandler() {
        while(true) {
            try {
                Thread.sleep(ThreadLocalRandom.current().nextInt(100, 3000));
            } catch (Exception e) { }

            timestampLock.writeLock().lock();
            GetTimestamp().Add(1);
            timestampLock.writeLock().unlock();

            TimestampedPayload payload = new TimestampedPayload();
            payload.GetTimestamp().Set(GetTimestamp());

            Send(GetRandomOutSocket(), payload);

        }
    }

    @Override
    void PayloadReceivedHandler(TimestampedPayload payload) {

        timestampLock.writeLock().lock();

        SimpleTimestamp t = payload.GetTimestamp();
        SimpleTimestamp newT = SimpleTimestamp.Max(t, GetTimestamp());
        newT.Add(1);

        Log("Received packet with timestamp " + t + ", current timestamp is " + GetTimestamp() + ".");
        GetTimestamp().Set(newT);
        Log("New timestamp is " + GetTimestamp() + ".");

        timestampLock.writeLock().unlock();

    }

}