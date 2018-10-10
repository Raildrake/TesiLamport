package lamport.timestamps;

import java.io.Serializable;

public class Timestamp implements Serializable {

    public final static Timestamp MAX_VALUE=new Timestamp(Integer.MAX_VALUE,Integer.MAX_VALUE,Integer.MAX_VALUE);
    public final static Timestamp MIN_VALUE=new Timestamp(Integer.MIN_VALUE,Integer.MIN_VALUE,Integer.MIN_VALUE);

    private int t=0;
    private int uid=-1; //di default a -1 per consistenza
    private int priority=-1;

    public Timestamp() {}
    public Timestamp(int uid) {
        this.uid=uid;
    }
    public Timestamp(int t, int uid) {
        this(uid);
        this.t=t;
    }
    public Timestamp(int t, int uid, int priority) {
        this(t,uid);
        this.priority=priority;
    }

    public void Add(int val) { t+=val; }

    public void AddPriority(int val) { priority+=val; }
    public void SubPriority(int val) {
        priority-=val;
        if (priority<1) priority=1;
    }

    public void Set(int val) { t=val; }
    public void Set(int t, int uid) {
        this.t=t;
        this.uid=uid;
    }
    public void Set(int t, int uid, int priority) {
        this.t=t;
        this.uid=uid;
        this.priority=priority;
    }
    public void Set(Timestamp val, boolean keepUID, boolean keepPriority) {
        t=val.t;
        if (!keepPriority) priority=val.priority;
        if (!keepUID) uid=val.uid;
    }
    public void Set(Timestamp val, boolean keepUID) {
        Set(val,keepUID,false);
    }
    //Set(val) copia interamente il timestamp, non usare per aggiornare il timestamp del processo locale!!
    public void Set(Timestamp val) {
        Set(val,false,false);
    }

    public boolean IsGreaterThan(Timestamp t2) {
        /*return priority>t2.priority ||
                (priority==t2.priority && t>t2.t) ||
                (t==t2.t && priority==t2.priority && uid>t2.uid);*/

        return t>t2.t ||
                (t==t2.t && priority>t2.priority) ||
                (t==t2.t && priority==t2.priority && uid>t2.uid);
    }
    public boolean IsEqualTo(Timestamp t2) { return t==t2.t && priority==t2.priority && uid==t2.uid; }

    public static Timestamp Max(Timestamp t1, Timestamp t2) {
        if (t1.IsGreaterThan(t2)) return t1.clone();
        return t2.clone();
    }

    @Override
    public String toString() {
        if (uid==-1) return "["+t+"]";
        else if (priority==-1) return "["+t+","+uid+"]";
        return "["+t+","+priority+","+uid+"]";
    }

    public Timestamp clone() {
        Timestamp c=new Timestamp();
        c.Set(this);
        return c;
    }
}
