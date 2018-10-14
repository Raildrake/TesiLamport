package lamport.datastore;

import javafx.util.Pair;
import lamport.payload.Payload;

import javax.xml.crypto.Data;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DataStore {

    private class LockedObject {
        private Lock lock;
        private Object obj;

        public LockedObject(Object obj, Lock lock) {
            this.obj=obj;
            this.lock=lock;
        }

        public Lock GetLock() { return lock; }
        public void SetLock(Lock l) { lock=l; }

        public Object GetObject() { return obj; }
        public void SetObject(Object obj) { this.obj=obj; }
    }

    private HashMap<String,LockedObject> data=new HashMap<>();

    public void Add(String name, Object value) {
        data.put(name,new LockedObject(value,new ReentrantLock()));
    }

    public boolean Exists(String n) {
        return data.containsKey(n);
    }
    public Object GetValue(String n) {
        if (!Exists(n)) return null;
        Object res=data.get(n).GetObject();
        return res;
    }
    public void SetValue(String n, Object value) {
        data.get(n).SetObject(value);
    }
    private Lock GetLock(String n) {
        return data.get(n).GetLock();
    }
    public void Lock(String n) {
        if (!Exists(n)) return;
        GetLock(n).lock();
    }
    public void Unlock(String n) {
        if (!Exists(n)) return;
        try {
            GetLock(n).unlock();
        } catch (Exception e) {}
    }

    public void Clear() {
        this.data=new HashMap<>();
    }

    public Set<String> GetKeys() {
        return data.keySet();
    }

}
