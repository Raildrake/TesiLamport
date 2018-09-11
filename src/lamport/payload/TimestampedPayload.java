package lamport.payload;

public class TimestampedPayload extends Payload {

    private int timestamp=0;

    public int GetTimestamp() { return timestamp; }
    public void SetTimestamp(int t) { timestamp = t; }

}
