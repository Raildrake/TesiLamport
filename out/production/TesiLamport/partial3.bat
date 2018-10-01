START cmd /c java lamport.boot.Main TotalLamport 15000 127.0.0.1:15001 127.0.0.1:15002
START cmd /c java lamport.boot.Main TotalLamport 15001 127.0.0.1:15000 127.0.0.1:15002
START cmd /c java lamport.boot.Main TotalLamport 15002 127.0.0.1:15000 127.0.0.1:15001