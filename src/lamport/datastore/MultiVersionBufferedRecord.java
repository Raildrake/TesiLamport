package lamport.datastore;

import lamport.payload.Payload;
import lamport.timestamps.Timestamp;

import java.util.PriorityQueue;

public class MultiVersionBufferedRecord extends MultiVersionRecord {

    private PriorityQueue<Payload> readBuffer=
            new PriorityQueue<>((p1, p2)->(p1.GetTimestamp().IsGreaterThan(p2.GetTimestamp())?1:0));
    private PriorityQueue<Payload> prewriteBuffer=
            new PriorityQueue<>((p1, p2)->(p1.GetTimestamp().IsGreaterThan(p2.GetTimestamp())?1:0));

    public PriorityQueue<Payload> GetReadBuffer() {return readBuffer;}
    public PriorityQueue<Payload> GetPrewriteBuffer() {return prewriteBuffer;}

    public boolean FallsInPrewriteInterval(Timestamp t) {
        for (Payload p : prewriteBuffer) {
            if (!t.IsGreaterThan(p.GetTimestamp())) continue; //mi servono solo quelli <t

            Timestamp tEnd=GetFirstWriteAfter(p.GetTimestamp());
            if (tEnd.IsGreaterThan(t)) return true; //perchè ho contemporaneamente che ts(p)<t<tEnd, cioè t appartiene a interval (per bernstein)
        }
        return false;
    }

    public MultiVersionBufferedRecord(int value) {
        super(value);
    }

}
