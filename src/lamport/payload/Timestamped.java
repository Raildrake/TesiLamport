package lamport.payload;

import lamport.timestamps.GenericTimestamp;

public interface Timestamped<T extends GenericTimestamp> {

    T GetTimestamp();

}
