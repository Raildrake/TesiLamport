package lamport.payload;

import lamport.timestamps.SimpleTimestamp;

public class TimestampedPayload extends Payload<SimpleTimestamp> {

    private SimpleTimestamp timestamp=new SimpleTimestamp();

    public SimpleTimestamp GetTimestamp() { return timestamp; }

    public void CopyFrom(TimestampedPayload src) {
        super.CopyFrom(src);
        this.timestamp=src.GetTimestamp();
    }
}
