package lamport.payload;

import java.io.*;
import java.net.Socket;

public abstract class Payload implements Serializable {

    public enum Request {
        VOID, READ, WRITE, PREWRITE,
        FAIL, SUCCESS_WRITE, SUCCESS_READ, BUFFERED_READ, BUFFERED_WRITE, BUFFERED_PREWRITE,
        READ_CANCEL, WRITE_CANCEL, PREWRITE_CANCEL, SUCCESS_CANCEL
    }

    private Request request = Request.VOID;
    private String target = "";
    private int arg1 = 0;
    private int host=0;
    private transient Socket usedSocket; //questa non la serializziamo, ci serve solo dopo aver inviato un payload per tenere traccia del socket usato

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


    public byte[] Encode() throws IOException {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        ObjectOutputStream objOut =new ObjectOutputStream(byteOut);
        objOut.writeObject(this);
        objOut.close();
        byteOut.close();
        return byteOut.toByteArray();
    }
    public static <T extends Payload> T Decode( InputStream input) throws IOException, ClassNotFoundException {
        T res;

        ObjectInputStream objIn=new ObjectInputStream(input);
        res=(T)objIn.readObject();

        return res;
    }

}
