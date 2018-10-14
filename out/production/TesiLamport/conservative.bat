START cmd /c java lamport.boot.Main Conservative 15005 -1 -1 -1 -1 127.0.0.1:15006 127.0.0.1:15007
START cmd /c java lamport.boot.Main Conservative 15006 -1 -1 -1 -1 127.0.0.1:15005 127.0.0.1:15007
START cmd /c java lamport.boot.Main Conservative 15007 -1 -1 -1 -1 127.0.0.1:15005 127.0.0.1:15006