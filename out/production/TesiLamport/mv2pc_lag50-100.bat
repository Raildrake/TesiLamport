START cmd /c java lamport.boot.Main MV2PC 15005 -1 -1 10 20 127.0.0.1:15006 127.0.0.1:15007
START cmd /c java lamport.boot.Main MV2PC 15006 -1 -1 10 20 127.0.0.1:15005 127.0.0.1:15007
START cmd /c java lamport.boot.Main MV2PC 15007 -1 -1 50 2500 127.0.0.1:15005 127.0.0.1:15006