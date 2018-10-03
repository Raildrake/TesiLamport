package lamport.timestamps;

import java.io.Serializable;

public class UniqueTimestamp extends GenericTimestamp implements Serializable {

    private int t=0;
    private int uid=-1;

    public UniqueTimestamp() {}
    public UniqueTimestamp(int uid) {
        this.uid=uid;
    }
    public UniqueTimestamp(int t, int uid) {
        this.t=t;
        this.uid=uid;
    }

    public void Add(int val) { t+=val; }
    public void Set(int val) { t=val; }
    public void Set(int t, int uid) {
        this.t=t;
        this.uid=uid;
    }
    public void Set(UniqueTimestamp val, boolean keepUID) {
        t=val.t;
        if (!keepUID) uid=val.uid;
    }

    public boolean IsGreaterThan(UniqueTimestamp t2) {
        return t>t2.t || (t==t2.t && uid>t2.uid);
    }
    public boolean IsEqualTo(UniqueTimestamp t2) { return t==t2.t && uid==t2.uid; }

    /* Ritorna una nuova istanza di SimpleTimestamp col valore più grande tra t1 e t2 */
    public static UniqueTimestamp Max(UniqueTimestamp t1, UniqueTimestamp t2) {
            if (t1.IsGreaterThan(t2)) return t1.clone();
            return t2.clone();
    }

    @Override
    public String toString() {
        return "("+t+","+uid+")";
    }

    public UniqueTimestamp clone() {
        UniqueTimestamp c=new UniqueTimestamp();
        c.Set(this,false);
        return c;
    }
}
