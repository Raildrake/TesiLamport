package lamport.timestamps;

import java.io.Serializable;

public class SimpleTimestamp implements Serializable {

    private int t=0;

    public void Add(int val) { t+=val; }
    public void Set(int val) { t=val; }
    public void Set(SimpleTimestamp val) { t=val.t; }

    public boolean IsGreaterThan(SimpleTimestamp t2) { return t>t2.t; }
    public boolean IsEqualTo(SimpleTimestamp t2) { return t==t2.t; }

    /* Ritorna una nuova istanza di SimpleTimestamp col valore più grande tra t1 e t2 */
    public static SimpleTimestamp Max(SimpleTimestamp t1, SimpleTimestamp t2) {
        if (t1.IsGreaterThan(t2)) return t1.clone();
        return t2.clone();
    }

    @Override
    public String toString() {
        return "("+t+")";
    }

    public SimpleTimestamp clone() {
        SimpleTimestamp c=new SimpleTimestamp();
        c.Set(this);
        return c;
    }
}
