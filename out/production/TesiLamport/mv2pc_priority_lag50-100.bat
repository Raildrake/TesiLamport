START cmd /c java lamport.boot.Main MV2PCP 15000 -1 -1 10 20 127.0.0.1:15001 127.0.0.1:15002
START cmd /c java lamport.boot.Main MV2PCP 15001 -1 -1 10 20 127.0.0.1:15000 127.0.0.1:15002
START cmd /c java lamport.boot.Main MV2PCP 15002 -1 -1 50 100 127.0.0.1:15000 127.0.0.1:15001