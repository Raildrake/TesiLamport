package lamport.process;

import lamport.datastore.DataStore;
import lamport.payload.Payload;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public abstract class Process<T extends Payload> {

    private int port;
    private int artificialDelayMin=0; //introduciamo un ritardo artificiale nella rete per amplificare gli effetti dell'accesso concorrente
    private int artificialDelayMax=0;
    private Thread listenThread;
    private Thread outThread;

    private DataStore dataStore=new DataStore();

    //private HashMap<Socket,Thread> outConnections=new HashMap<>();
    private List<Socket> outSockets=new LinkedList<>();
    private HashMap<Socket,Thread> inConnections=new HashMap<>();

    public Process(int port) {
        this.port=port;
    }
    public void Start() {
        outThread=new Thread(()->OutputHandler());
        outThread.start();
    }

    public void SetArtificialDelay(int min, int max) {
        artificialDelayMin=min;
        artificialDelayMax=max;
    }
    public int GetPort() { return port; }
    public DataStore GetDataStore() { return dataStore; }
    protected List<Socket> GetOutSockets() { return outSockets; }
    protected Socket GetRandomOutSocket() {
        List<Socket> socks=GetOutSockets();
        return socks.get(ThreadLocalRandom.current().nextInt(0,socks.size()));
    }

    public boolean Connect(String host, int port) {
        Log("Attempting to connect to " + host + ":" + port + "...");
        try {
            Socket s = new Socket(host, port);

            if (!s.isConnected())
                return false;

            Log("Connection established with " + host + ":" + port);
            outSockets.add(s);

            return true;
        } catch (IOException e) {
            return false;
        }
    }
    public void Listen() {
        listenThread=new Thread(()-> {
            try {
                ServerSocket s = new ServerSocket(GetPort());
                Log("Started listening on port "+GetPort()+"...");

                while(true) {
                    Socket conn=s.accept();
                    Log("Accepted connection from "+conn.getRemoteSocketAddress());
                    Thread iT=new Thread(()->InputSocketHandler(conn)); //come sopra, anche se questo è il socket da cui ci aspettiamo l'input, si tratta ugualmente di un canale a due vie
                    iT.start();
                    inConnections.put(conn,iT);
                }

            } catch (Exception e) { e.printStackTrace(); }
        });
        listenThread.start();
    }

    public void Send(Socket s, Payload p) {
        try {
            BufferedOutputStream bos=new BufferedOutputStream(s.getOutputStream());
            bos.write(p.Encode());
            bos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    protected T Receive(Socket s) {
        try {
            BufferedInputStream bis=new BufferedInputStream((s.getInputStream()));
            return T.Decode(bis);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    abstract void OutputHandler();
    void InputSocketHandler(Socket s) {
        while(true) {
            T payload=Receive(s);
            try {
                Thread.sleep(ThreadLocalRandom.current().nextInt(artificialDelayMin, artificialDelayMax));
            } catch (Exception e) { }
            PayloadReceivedHandler(s,payload);
        }
    }
    abstract void PayloadReceivedHandler(Socket s, T payload);

    protected void Log(Object msg) {
        System.out.println("["+(new Date()).toString()+"] HOST"+this.GetPort()+": "+msg);
    }
}
