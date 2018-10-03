START cmd /c java lamport.boot.Main Simple2PC 15000 10 100 127.0.0.1:15001 127.0.0.1:15002
START cmd /c java lamport.boot.Main Simple2PC 15001 30 50 127.0.0.1:15000 127.0.0.1:15002
START cmd /c java lamport.boot.Main Simple2PC 15002 30 50 127.0.0.1:15000 127.0.0.1:15001