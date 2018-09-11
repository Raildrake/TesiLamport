package lamport.datastore;

import javafx.util.Pair;
import lamport.payload.Payload;

import javax.xml.crypto.Data;
import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DataStore {

    public class DataOutput {
        private boolean success;
        private int value;

        public DataOutput(boolean success) {
            this.success=success;
        }
        public DataOutput(boolean success, int value) {
            this(success);
            this.value=value;
        }

        public boolean WasSuccessful() { return success; }
        public int GetValue() { return value; }
    }

    private HashMap<String,Pair<Integer,Lock>> data=new HashMap<>();

    public void Add(String name, int value) {
        data.put(name,new Pair<>(value,new ReentrantLock()));
    }

    private boolean Exists(String n) {
        return data.containsKey(n);
    }
    private int GetValue(String n) {
        return data.get(n).getKey();
    }
    private void SetValue(String n, int value) {
        Lock lock=GetLock(n);
        data.remove(n);
        data.put(n,new Pair<>(value,lock)); //TODO: usare alternativa a Pair che sia più flessibile
    }
    private Lock GetLock(String n) {
        return data.get(n).getValue();
    }

    public DataOutput ProcessPayload(Payload p) {
        if (!Exists(p.GetTarget())) return new DataOutput(false);

        Lock targetLock=GetLock(p.GetTarget());
        targetLock.lock();

        switch (p.GetRequest()) {
            case VOID:
                targetLock.unlock();
                return new DataOutput(true);
            case GET:
                int res=GetValue(p.GetTarget());
                targetLock.unlock();
                return new DataOutput(true,res);
            case SET:
                SetValue(p.GetTarget(),p.GetArg1());
                targetLock.unlock();
                return new DataOutput(true);
        }

        targetLock.unlock();
        return new DataOutput(false);
    }
}
