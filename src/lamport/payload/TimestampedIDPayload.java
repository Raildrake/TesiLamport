package lamport.payload;

import javafx.util.Pair;

public class TimestampedIDPayload extends Payload {

    private int[] timestamp=new int[]{0,0};

    public int[] GetTimestamp() { return timestamp; }

    public void SetTimestamp(int[] t) { timestamp = t; }
    public void SetTimestamp(int time) { timestamp[0] = time; }
    public void SetTimestamp(int time, int id) { timestamp[0] = time; timestamp[1] = id; }

    public boolean HappenedBefore(int[] otherTimestamp) {
        if (GetTimestamp()[0]<otherTimestamp[0]) return true;
        if (GetTimestamp()[0]==otherTimestamp[0] && GetTimestamp()[1]<otherTimestamp[1]) return true;
        return false;
    }

}
