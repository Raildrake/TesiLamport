START cmd /c java lamport.boot.Main Simple1PC 15000 127.0.0.1:15001 127.0.0.1:15002
START cmd /c java lamport.boot.Main Simple1PC 15001 127.0.0.1:15000 127.0.0.1:15002
START cmd /c java lamport.boot.Main Simple1PC 15002 127.0.0.1:15000 127.0.0.1:15001