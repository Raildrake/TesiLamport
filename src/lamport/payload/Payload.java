package lamport.payload;

import java.io.*;

public abstract class Payload implements Serializable {

    public enum Request { VOID, GET, SET }

    private Request request = Request.VOID;
    private String target = "";
    private int arg1 = 0;

    public Request GetRequest() { return request; }
    public void SetRequest(Request req) { request=req; }

    public String GetTarget() { return target; }
    public void SetTarget(String t) { target=t; }

    public int GetArg1() { return arg1; }
    public void SetArg1(int val) { arg1=val; }


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
