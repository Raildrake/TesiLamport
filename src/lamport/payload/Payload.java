package lamport.payload;

import lamport.timestamps.Timestamp;

import java.io.*;
import java.net.Socket;

public class Payload implements Serializable {
    private Request request = Request.VOID;
    private String target = "";
    private int arg1 = 0;
    private int host=0;
    private Timestamp timestamp=new Timestamp();
    private transient Socket usedSocket; //questa non la serializziamo, ci serve solo dopo aver inviato/ricevuto un payload per tenere traccia del socket usato localmente

    public Request GetRequest() { return request; }
    public void SetRequest(Request req) { request=req; }

    public String GetTarget() { return target; }
    public void SetTarget(String t) { target=t; }

    public int GetArg1() { return arg1; }
    public void SetArg1(int val) { arg1=val; }

    public int GetHost() { return host; }
    public void SetHost(int val) { host=val; }

    public Socket GetUsedSocket() { return usedSocket; }
    public void SetUsedSocket(Socket val) { usedSocket=val; }

    public Timestamp GetTimestamp() { return timestamp; }


    public void Encode(ObjectOutputStream out) throws IOException {
        out.writeObject(this);
        out.flush();
    }
    public static <T extends Payload> T Decode(ObjectInputStream input) throws IOException, ClassNotFoundException {
        T res;

        res=(T)input.readObject();

        return res;
    }

    public void CopyFrom(Payload src) {
        SetRequest(src.GetRequest());
        SetTarget(src.GetTarget());
        SetArg1(src.GetArg1());
        SetHost(src.GetHost());
        SetUsedSocket(src.GetUsedSocket());
        GetTimestamp().Set(src.GetTimestamp());
    }

}
