package lamport.boot;

import javafx.util.Pair;
import lamport.process.*;

import java.util.LinkedList;
import java.util.List;

public class Main {

    public static void main(String[] args) {
        //Formato args
        //main.java processType listenPort targetHost1:targetPort1 targetHost2:targetPort2 ...
        if (args.length<2) {
            System.out.println("Not enough parameters.");
            return;
        }

        String processType=args[0];
        int listenPort=Integer.parseInt(args[1]);
        List<Pair<String,Integer>> targetHosts=new LinkedList<>();

        for (int k=2;k<args.length;k++) {
            String[] parameter=args[k].split(":");
            String host=parameter[0];
            int port=Integer.parseInt(parameter[1]);
            targetHosts.add(new Pair<>(host,port));
        }

        lamport.process.Process process=null;

        switch (processType) {
            case "NoSync": process=new ProcessNoSync(listenPort); break;
            case "PartialLamport": process=new ProcessPartialLamport(listenPort); break;
            case "TotalLamport": process=new ProcessTotalLamport(listenPort); break;
            case "Simple1PC": process=new ProcessSimple1PC(listenPort); break;
            case "Simple2PC": process=new ProcessSimple2PC(listenPort); break;
        }
        process.SetArtificialDelay(100,1000);
        process.Listen();

        while (targetHosts.size()>0) {
            if (process.Connect(targetHosts.get(0).getKey(), targetHosts.get(0).getValue()))
                targetHosts.remove(0);
        }
        process.Start();

    }
}
