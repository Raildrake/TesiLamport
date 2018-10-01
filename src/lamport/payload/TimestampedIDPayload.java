package lamport.payload;

import lamport.timestamps.UniqueTimestamp;

public class TimestampedIDPayload extends Payload {

    private UniqueTimestamp timestamp=new UniqueTimestamp();

    public UniqueTimestamp GetTimestamp() { return timestamp; }

}
