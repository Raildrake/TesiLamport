package lamport.process;

import lamport.datastore.TimeBufferedRecord;
import lamport.payload.Payload;
import lamport.payload.Request;
import lamport.payload.TimestampedIDPayload;
import lamport.timestamps.UniqueTimestamp;

import java.util.LinkedList;

public class TestProcess extends ProcessSimple1PC {

    private boolean client;
    private int count=0;

    public TestProcess(int port, boolean client) {
        super(port);
        this.client=client;
    }

    @Override
    void OutputHandler() {
        if (!client) return;
        while(true) {
            timestampLock.readLock().lock();
            UniqueTimestamp curTimestamp = GetTimestamp().clone();
            timestampLock.readLock().unlock();

            count++;
            Log("Sending request "+count);
            //SendAndWaitResponse(curTimestamp,GetRandomOutSocket(),Payload.Request.WRITE,"A",-1);
            Log("Received response to "+count);
            //Receive(GetRandomOutSocket());
            Log("Received response 2 to "+count);
        }
    }

    @Override
    void PayloadReceivedHandler(TimestampedIDPayload payload) {
        if (!client) {
            timestampLock.readLock().lock();
            UniqueTimestamp curTimestamp = GetTimestamp().clone();
            timestampLock.readLock().unlock();

            if (payload.GetRequest()==Request.WRITE) {
                count++;
                Log("Sending response "+count);
                SendPayload(curTimestamp, payload.GetUsedSocket(),Request.BUFFERED_WRITE,"A",-1);
                Log("Sent response "+count);
                SendPayload(curTimestamp, payload.GetUsedSocket(),Request.SUCCESS_WRITE,"A",-1);
                Log("Sent response 2 for "+count);
            }
        }
    }

    @Override
    protected String CurTime() {
        return GetTimestamp().toString();
    }
}
