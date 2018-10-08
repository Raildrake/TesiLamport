package lamport.process;

import lamport.payload.Payload;
import lamport.timestamps.Timestamp;

import java.net.Socket;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ProcessPartialLamport extends Process {

    public ProcessPartialLamport(int port) {
        super(port);
    }

    @Override
    void OutputHandler() {
        while(true) {
            try {
                Thread.sleep(ThreadLocalRandom.current().nextInt(100, 3000));
            } catch (Exception e) { }

            timestampLock.writeLock().lock();
            GetTimestamp().Add(1);
            timestampLock.writeLock().unlock();

            Payload payload = new Payload();
            payload.GetTimestamp().Set(GetTimestamp());

            Send(GetRandomOutSocket(), payload);

        }
    }

    @Override
    void PayloadReceivedHandler(Payload payload) {

        timestampLock.writeLock().lock();

        Timestamp t = payload.GetTimestamp();
        Timestamp newT = Timestamp.Max(t, GetTimestamp());
        newT.Add(1);

        Log("Received packet with timestamp " + t + ", current timestamp is " + GetTimestamp() + ".");
        GetTimestamp().Set(newT);
        Log("New timestamp is " + GetTimestamp() + ".");

        timestampLock.writeLock().unlock();

    }

}