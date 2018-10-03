package lamport.process;

import lamport.datastore.DataStore;
import lamport.payload.Payload;
import lamport.payload.Request;
import lamport.payload.TimestampedIDPayload;
import lamport.timestamps.GenericTimestamp;
import lamport.timestamps.UniqueTimestamp;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.Predicate;

public abstract class Process<T extends Payload<Z>, Z extends GenericTimestamp> {

    private int port;
    private int artificialDelayMin=0; //introduciamo un ritardo artificiale nella rete per amplificare gli effetti dell'accesso concorrente
    private int artificialDelayMax=0;
    private Thread listenThread;
    private Thread outThread;

    private boolean debug=false;

    private DataStore dataStore=new DataStore();

    private List<Socket> outSockets=new LinkedList<>();
    private List<T> responseQueue=new LinkedList<>();
    private HashMap<Socket,Thread> inConnections=new HashMap<>();
    private HashMap<Socket,Thread> outSocketReaders=new HashMap<>();

    private HashMap<Socket,ObjectInputStream> socketOIS=new HashMap<>();
    private HashMap<Socket,ObjectOutputStream> socketOOS=new HashMap<>();

    private Z timestamp;


    public Process(int port, Class<Z> timeClass) {
        this.port=port;
        try {
            this.timestamp = timeClass.newInstance();
        } catch (Exception e) { }
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
    protected boolean IsDebug() { return debug; }

    protected Z GetTimestamp() { return timestamp; }

    public boolean Connect(String host, int port) {
        Log("Attempting to connect to " + host + ":" + port + "...");
        try {
            Socket s = new Socket(host, port);

            if (!s.isConnected())
                return false;

            Log("Connection established with " + host + ":" + port);
            outSockets.add(s);
            socketOIS.put(s,new ObjectInputStream(s.getInputStream()));
            socketOOS.put(s,new ObjectOutputStream(s.getOutputStream()));
            socketOOS.get(s).flush();

            Thread t=new Thread(()->ResponseSocketReader(s));
            t.start();
            outSocketReaders.put(s,t);

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

                    socketOOS.put(conn,new ObjectOutputStream(conn.getOutputStream()));
                    socketOOS.get(conn).flush();
                    socketOIS.put(conn,new ObjectInputStream(conn.getInputStream()));
                }

            } catch (Exception e) { e.printStackTrace(); }
        });
        listenThread.start();
    }

    public void Send(Socket s, Payload p) {
        try {
            //Thread.sleep(ThreadLocalRandom.current().nextInt(artificialDelayMin, artificialDelayMax));

            p.SetHost(GetPort());
            p.SetUsedSocket(s);

            synchronized (s.getOutputStream()) {
                p.Encode(socketOOS.get(s));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private T Receive(Socket s) {
        try {
            //Thread.sleep(ThreadLocalRandom.current().nextInt(artificialDelayMin, artificialDelayMax));

            T res;
            synchronized (s.getInputStream()) {
                res = T.Decode(socketOIS.get(s));
            }
            res.SetUsedSocket(s);
            return res;
        } catch (Exception e) {
            //e.printStackTrace();
        }
        return null;
    }

    void ResponseSocketReader(Socket s) {
        while(true) {
            T payload=Receive(s);
            if (payload!=null) {
                responseQueue.add(payload);
                ProcessTimestamp(payload.GetTimestamp());
            }
        }
    }
    abstract void OutputHandler();
    void InputSocketHandler(Socket s) {
        while(true) {
            T payload=Receive(s);
            if (payload!=null) {
                PayloadReceivedHandler(payload);
                ProcessTimestamp(payload.GetTimestamp());
            }
        }
    }
    abstract void PayloadReceivedHandler(T payload);

    void ProcessTimestamp(Z timestamp) {
        //comportamento default ignora totalmente l'aggiornamento del timestamp
    }

    T WaitResponse(Predicate<T> criteria) {
        while(true) {
            try {
                for (int k=0;k<responseQueue.size();k++) {
                    if (criteria.test(responseQueue.get(k))) {
                        return responseQueue.remove(k);
                    }
                }

                Thread.sleep(1);
            } catch (Exception e) { e.printStackTrace(); }
        }
    }
    T WaitResponse(Socket fromSocket, Request[] possibleResponses, String target) {
        return WaitResponse(p->(p.GetUsedSocket().equals(fromSocket) && Arrays.asList(possibleResponses).contains(p.GetRequest()) && p.GetTarget().equals(target)));
    }


    protected void Log(Object msg) {
        System.out.println(CurTime()+" HOST"+this.GetPort()+": "+msg);
    }
    protected void Debug(Object msg) {
        if (IsDebug()) Log(msg);
    }
    protected String CurTime() {
        return "["+(new Date()).toString()+"]";
    }
}
