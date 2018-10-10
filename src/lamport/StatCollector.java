package lamport;

import java.text.DecimalFormat;
import java.util.Date;

public class StatCollector {

    private int failedCount=0;
    private int successCount=0;
    private Date attemptStart;
    private long attemptTimeTotal=0;
    private boolean lastFail=false;
    private int sequentialFails=0;

    public void Fail() {
        if (lastFail) sequentialFails++;
        failedCount++;
        lastFail=true;
    }
    public void Success() {
        successCount++;
        lastFail=false;
        EndAttempt();
    }
    public void StartAttempt() {
        attemptStart=new Date();
    }
    public void EndAttempt() {
        Date now=new Date();
        long diff=(now.getTime()-attemptStart.getTime());
        attemptTimeTotal+=diff;
    }

    public int GetFailCount() { return failedCount; }
    public int GetSuccessCount() { return successCount; }
    public int GetTotalCount() { return failedCount+successCount; }

    public double GetAvgDelay() {
        return (double)attemptTimeTotal/(double)successCount;
    }
    public double GetAvgSequentialFails() {
        return (double)sequentialFails/(double)GetFailCount();
    }

    protected double GetSuccessPercent() {
        return ((double)GetSuccessCount()/(double)GetTotalCount())*100.0;
    }

    public String RoundToString(double val) {
        return new DecimalFormat("##.000").format(val);
    }

    public String GetStats() {
        return "(" + GetSuccessCount() + "/" + GetTotalCount() + ") [" + RoundToString(GetSuccessPercent())
                + "%] - Avg delay: "+RoundToString(GetAvgDelay())+"ms, Seq fails: "+RoundToString(GetAvgSequentialFails());
    }
}
