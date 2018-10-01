START cmd /c java lamport.boot.Main PartialLamport 15000 127.0.0.1:15001 127.0.0.1:15002
START cmd /c java lamport.boot.Main PartialLamport 15001 127.0.0.1:15000 127.0.0.1:15002
START cmd /c java lamport.boot.Main PartialLamport 15002 127.0.0.1:15000 127.0.0.1:15001