START cmd /c java lamport.boot.Main MV1PC 15000 -1 -1 -1 -1 127.0.0.1:15001 127.0.0.1:15002
START cmd /c java lamport.boot.Main MV1PC 15001 -1 -1 -1 -1 127.0.0.1:15000 127.0.0.1:15002
START cmd /c java lamport.boot.Main MV1PC 15002 -1 -1 -1 -1 127.0.0.1:15000 127.0.0.1:15001