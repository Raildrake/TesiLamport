START cmd /c java lamport.boot.Main TestClient 15000 10 100 127.0.0.1:15001
START cmd /c java lamport.boot.Main TestServer 15001 30 50 127.0.0.1:15000 127.0.0.1:15002
START cmd /c java lamport.boot.Main TestClient 15002 10 100 127.0.0.1:15001