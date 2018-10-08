package lamport.datastore;

import lamport.timestamps.Timestamp;

public class TimestampedRecord {

    private int value;
    private Timestamp w_ts=new Timestamp(-1,-1);
    private Timestamp r_ts=new Timestamp(-1,-1);

    public int GetValue() { return value; }
    public void SetValue(int v) { value=v; }

    public Timestamp GetW_TS() { return w_ts; }
    public void SetW_TS(Timestamp v) { w_ts=v; }

    public Timestamp GetR_TS() { return r_ts; }
    public void SetR_TS(Timestamp v) { r_ts=v; }

    public TimestampedRecord(int value) { this.value=value; }

}
