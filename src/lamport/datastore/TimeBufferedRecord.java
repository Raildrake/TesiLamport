package lamport.datastore;

import lamport.payload.TimestampedIDPayload;
import lamport.timestamps.UniqueTimestamp;

import java.util.PriorityQueue;

public class TimeBufferedRecord {

    private int value;
    private UniqueTimestamp w_ts=new UniqueTimestamp(-1,-1);
    private UniqueTimestamp r_ts=new UniqueTimestamp(-1,-1);

    private PriorityQueue<TimestampedIDPayload> readBuffer=
            new PriorityQueue<>((TimestampedIDPayload p1, TimestampedIDPayload p2)->(p1.GetTimestamp().IsGreaterThan(p2.GetTimestamp())?1:0));
    private PriorityQueue<TimestampedIDPayload> writeBuffer=
            new PriorityQueue<>((TimestampedIDPayload p1, TimestampedIDPayload p2)->(p1.GetTimestamp().IsGreaterThan(p2.GetTimestamp())?1:0));
    private PriorityQueue<TimestampedIDPayload> prewriteBuffer=
            new PriorityQueue<>((TimestampedIDPayload p1, TimestampedIDPayload p2)->(p1.GetTimestamp().IsGreaterThan(p2.GetTimestamp())?1:0));

    public int GetValue() { return value; }
    public void SetValue(int v) { value=v; }

    public UniqueTimestamp GetW_TS() { return w_ts; }
    public void SetW_TS(UniqueTimestamp v) { w_ts=v; }

    public UniqueTimestamp GetR_TS() { return r_ts; }
    public void SetR_TS(UniqueTimestamp v) { r_ts=v; }

    public PriorityQueue<TimestampedIDPayload> GetReadBuffer() {return readBuffer;}
    public PriorityQueue<TimestampedIDPayload> GetWriteBuffer() {return writeBuffer;}
    public PriorityQueue<TimestampedIDPayload> GetPrewriteBuffer() {return prewriteBuffer;}

    public UniqueTimestamp GetMinR_TS() {
        if (GetReadBuffer().isEmpty()) return new UniqueTimestamp(Integer.MAX_VALUE,-1);
        return GetReadBuffer().peek().GetTimestamp();
    }
    public UniqueTimestamp GetMinW_TS() {
        if (GetWriteBuffer().isEmpty()) return new UniqueTimestamp(Integer.MAX_VALUE,-1);
        return GetWriteBuffer().peek().GetTimestamp();
    }
    public UniqueTimestamp GetMinP_TS() {
        if (GetPrewriteBuffer().isEmpty()) return new UniqueTimestamp(Integer.MAX_VALUE,-1);
        return GetPrewriteBuffer().peek().GetTimestamp();
    }


    public TimeBufferedRecord(int value) { this.value=value; }

}
