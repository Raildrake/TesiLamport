package lamport.datastore;

import lamport.payload.Payload;
import lamport.timestamps.Timestamp;

import java.util.PriorityQueue;

public class TimeBufferedRecord extends TimestampedRecord {

    private PriorityQueue<Payload> readBuffer=
            new PriorityQueue<>((p1, p2)->(p1.GetTimestamp().IsGreaterThan(p2.GetTimestamp())?1:0));
    private PriorityQueue<Payload> writeBuffer=
            new PriorityQueue<>((p1, p2)->(p1.GetTimestamp().IsGreaterThan(p2.GetTimestamp())?1:0));
    private PriorityQueue<Payload> prewriteBuffer=
            new PriorityQueue<>((p1, p2)->(p1.GetTimestamp().IsGreaterThan(p2.GetTimestamp())?1:0));

    public PriorityQueue<Payload> GetReadBuffer() {return readBuffer;}
    public PriorityQueue<Payload> GetWriteBuffer() {return writeBuffer;}
    public PriorityQueue<Payload> GetPrewriteBuffer() {return prewriteBuffer;}

    public Timestamp GetMinR_TS() {
        if (GetReadBuffer().isEmpty()) return new Timestamp(Integer.MAX_VALUE,Integer.MAX_VALUE,Integer.MAX_VALUE);
        return GetReadBuffer().peek().GetTimestamp();
    }
    public Timestamp GetMinW_TS() {
        if (GetWriteBuffer().isEmpty()) return new Timestamp(Integer.MAX_VALUE,Integer.MAX_VALUE,Integer.MAX_VALUE);
        return GetWriteBuffer().peek().GetTimestamp();
    }
    public Timestamp GetMinP_TS() {
        if (GetPrewriteBuffer().isEmpty()) return new Timestamp(Integer.MAX_VALUE,Integer.MAX_VALUE,Integer.MAX_VALUE);
        return GetPrewriteBuffer().peek().GetTimestamp();
    }

    public TimeBufferedRecord(int value) {
        super(value);
    }

}
