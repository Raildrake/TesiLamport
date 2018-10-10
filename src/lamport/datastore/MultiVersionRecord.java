package lamport.datastore;

import javafx.util.Pair;
import lamport.timestamps.Timestamp;

import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.TreeSet;

public class MultiVersionRecord {

    private TreeSet<Pair<Timestamp, Integer>> versions=new TreeSet<>(
            (p1, p2)->(p1.getKey().IsGreaterThan(p2.getKey())?1:0));
    private TreeSet<Timestamp> r_ts=new TreeSet<>(
            (p1, p2)->(p1.IsGreaterThan(p2)?1:0));

    public TreeSet<Pair<Timestamp, Integer>> GetVersions() { return versions; }
    public TreeSet<Timestamp> GetR_TS() { return r_ts; }

    public void Read(Timestamp t) { GetR_TS().add(t); }
    public void Write(int val, Timestamp t) { GetVersions().add(new Pair<>(t,val)); }

    public int GetOldestVersionBefore(Timestamp t) {
        Pair<Timestamp, Integer> res=GetVersions().floor(new Pair<>(t,-1));
        return res.getValue(); //non può essere null perchè abbiamo almeno il valore iniziale
    }
    public Timestamp GetFirstWriteAfter(Timestamp t) {
        Pair<Timestamp, Integer> res=GetVersions().ceiling(new Pair<>(t,-1));
        if (res==null) return Timestamp.MAX_VALUE;
        return res.getKey();
    }
    public boolean HasReadInInterval(Timestamp tBegin, Timestamp tEnd) {
        return !GetR_TS().subSet(tBegin,tEnd).isEmpty();
    }

    public MultiVersionRecord(int value) { Write(value,Timestamp.MIN_VALUE); }

}
