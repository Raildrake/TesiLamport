package lamport.process;

import lamport.payload.Payload;
import lamport.timestamps.Timestamp;

import java.util.concurrent.ThreadLocalRandom;

public class ProcessTotalLamport extends Process {

    public ProcessTotalLamport(int port) {
        super(port);
        GetTimestamp().Set(0,port); //ecco, qui invece vogliamo proprio impostarlo manualmente!
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
            payload.GetTimestamp().Set(GetTimestamp(),false);

            Send(GetRandomOutSocket(), payload);

        }
    }

    @Override
    void PayloadReceivedHandler(Payload payload) {

        timestampLock.writeLock().lock();

        Timestamp t = payload.GetTimestamp();
        Timestamp newT = Timestamp.Max(t, GetTimestamp());
        newT.Add(1);
        Log("Received packet with timestamp " + payload.GetTimestamp() + ", current timestamp is " + GetTimestamp() + ".");
        GetTimestamp().Set(newT, true);
        Log("New timestamp is " + GetTimestamp() + ".");

        timestampLock.writeLock().unlock();

    }

}