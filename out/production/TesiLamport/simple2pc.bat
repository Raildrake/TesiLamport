START cmd /c java lamport.boot.Main Simple2PC 15005 -1 -1 -1 -1 127.0.0.1:15006 127.0.0.1:15007
START cmd /c java lamport.boot.Main Simple2PC 15006 -1 -1 -1 -1 127.0.0.1:15005 127.0.0.1:15007
START cmd /c java lamport.boot.Main Simple2PC 15007 -1 -1 -1 -1 127.0.0.1:15005 127.0.0.1:15006