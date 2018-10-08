package lamport.process;

import lamport.payload.Payload;
import lamport.timestamps.Timestamp;

import java.util.concurrent.ThreadLocalRandom;

public class ProcessNoSync extends Process {

    public ProcessNoSync(int port) { super(port); }

    @Override
    void OutputHandler() {
        while(true) {
            try {
                Thread.sleep(ThreadLocalRandom.current().nextInt(100, 3000));
            } catch (Exception e) { }

            synchronized(GetTimestamp()) {
                GetTimestamp().Add(1);
            }

            Payload payload = new Payload();
            payload.GetTimestamp().Set(GetTimestamp());

            Send(GetRandomOutSocket(), payload);

        }
    }

    @Override
    void PayloadReceivedHandler(Payload payload) {

        timestampLock.readLock().lock();

        Timestamp t=payload.GetTimestamp();
        Log("Received packet with timestamp "+t+", current timestamp is "+GetTimestamp()+".");

        timestampLock.readLock().unlock();

    }

}