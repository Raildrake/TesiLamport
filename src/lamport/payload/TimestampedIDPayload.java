package lamport.payload;

import lamport.timestamps.UniqueTimestamp;

public class TimestampedIDPayload extends Payload<UniqueTimestamp> {

    private UniqueTimestamp timestamp=new UniqueTimestamp();

    public UniqueTimestamp GetTimestamp() { return timestamp; }

    public void CopyFrom(TimestampedIDPayload src) {
        super.CopyFrom(src);
        this.timestamp=src.GetTimestamp();
    }
}
