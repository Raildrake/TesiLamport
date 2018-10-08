package lamport.datastore;

import lamport.payload.Payload;
import lamport.timestamps.Timestamp;

import java.util.PriorityQueue;

public class TimeBufferedRecord {

    private int value;
    private Timestamp w_ts=new Timestamp(-1,-1,-1);
    private Timestamp r_ts=new Timestamp(-1,-1,-1);

    private PriorityQueue<Payload> readBuffer=
            new PriorityQueue<>((Payload p1, Payload p2)->(p1.GetTimestamp().IsGreaterThan(p2.GetTimestamp())?1:0));
    private PriorityQueue<Payload> writeBuffer=
            new PriorityQueue<>((Payload p1, Payload p2)->(p1.GetTimestamp().IsGreaterThan(p2.GetTimestamp())?1:0));
    private PriorityQueue<Payload> prewriteBuffer=
            new PriorityQueue<>((Payload p1, Payload p2)->(p1.GetTimestamp().IsGreaterThan(p2.GetTimestamp())?1:0));

    public int GetValue() { return value; }
    public void SetValue(int v) { value=v; }

    public Timestamp GetW_TS() { return w_ts; }
    public void SetW_TS(Timestamp v) { w_ts=v; }

    public Timestamp GetR_TS() { return r_ts; }
    public void SetR_TS(Timestamp v) { r_ts=v; }

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
        this.value=value;
    }

}
