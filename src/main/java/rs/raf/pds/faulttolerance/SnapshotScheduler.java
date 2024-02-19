package rs.raf.pds.faulttolerance;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SnapshotScheduler {
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public void startSnapshotRoutine(AccountService accountService, ReplicatedLog replicatedLog, long interval, TimeUnit timeUnit,String fileName) {
        Runnable task = new Runnable() {
            public void run() {
                accountService.takeSnapshot();
                replicatedLog.takeSnapshot();
                System.out.println("Snapshots taken successfully");
            }
        };

        scheduler.scheduleAtFixedRate(task, 0, interval, timeUnit);
    }
}
