package lamport.datastore;

import lamport.timestamps.UniqueTimestamp;

public class TimestampedRecord {

    private int value;
    private UniqueTimestamp w_ts=new UniqueTimestamp(-1,-1);
    private UniqueTimestamp r_ts=new UniqueTimestamp(-1,-1);

    public int GetValue() { return value; }
    public void SetValue(int v) { value=v; }

    public UniqueTimestamp GetW_TS() { return w_ts; }
    public void SetW_TS(UniqueTimestamp v) { w_ts=v; }

    public UniqueTimestamp GetR_TS() { return r_ts; }
    public void SetR_TS(UniqueTimestamp v) { r_ts=v; }

    public TimestampedRecord(int value) { this.value=value; }

}
