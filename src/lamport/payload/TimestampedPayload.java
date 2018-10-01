package lamport.payload;

import lamport.timestamps.SimpleTimestamp;

public class TimestampedPayload extends Payload {

    private SimpleTimestamp timestamp=new SimpleTimestamp();

    public SimpleTimestamp GetTimestamp() { return timestamp; }

}
