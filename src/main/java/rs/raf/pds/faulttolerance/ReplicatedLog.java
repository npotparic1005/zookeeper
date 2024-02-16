package rs.raf.pds.faulttolerance;

import rs.raf.pds.faulttolerance.gRPC.LogEntry;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class ReplicatedLog {

	public static interface LogReplicator {
		public void replicateOnFollowers(Long entryAtIndex, byte[] data);
		//void requestMissingEntries(Long followerLastIndex);  // Added method to request missing entries

	}
	
	Long lastLogEntryIndex = 0L;
	final LogReplicator node;
	FileOutputStream fs;
	OutputStreamWriter writer;
	
	public ReplicatedLog(String fileName, LogReplicator node) throws FileNotFoundException {
		this.node = node;
		fs = new FileOutputStream(fileName);
		writer = new OutputStreamWriter(fs);
	}
		
	public void appendAndReplicate(byte[] data) throws IOException {
		Long lastLogEntryIndex = appendToLocalLog(data);
		node.replicateOnFollowers(lastLogEntryIndex, data);  
	}
	
	protected Long appendToLocalLog(byte[] data) throws IOException {
		String s = new String(data);
		System.out.println("Log #"+lastLogEntryIndex+":"+s);
		
		//fs.write(data);
		//fs.flush();
		writer.write(s);writer.write("\r\n");
		writer.flush();
		fs.flush();
		
		return ++lastLogEntryIndex;
	}

	protected Long getLastLogEntryIndex() {
		return lastLogEntryIndex;
	}

	public void handleFollowerRequest(Long followerLastIndex) throws IOException {
		if (followerLastIndex < lastLogEntryIndex) {
			// Assuming there's a method to fetch log entries from startIndex to endIndex
			byte[][] missingEntries = fetchLogEntries(followerLastIndex + 1, lastLogEntryIndex);
			for (byte[] entry : missingEntries) {
				// Assuming replicateOnFollowers can handle batch entries
				node.replicateOnFollowers(++followerLastIndex, entry);
			}
		}
	}
	// Method to fetch log entries, implementation depends on how logs are stored
	protected byte[][] fetchLogEntries(Long startIndex, Long endIndex) {
		// Implementation to fetch log entries from startIndex to endIndex
		return new byte[0][];
	}
	//uzimanje trenutnog stanja za snapshot
	public List<LogEntry> getCurrentState() {
		List<LogEntry> entries = new ArrayList<>();
		try (FileInputStream input = new FileInputStream("fs")) {
			while (input.available() > 0) {
				// Assuming log entries are delimited
				LogEntry entry = LogEntry.parseDelimitedFrom(input);
				if (entry != null) {
					entries.add(entry);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return entries;
	}
	public synchronized void takeSnapshot() {
		try {
			// Assuming ReplicatedLog has a method to get the current state or entries
			List<LogEntry> currentState = this.getCurrentState();
			FileOutputStream fileOut = new FileOutputStream("replicatedLogSnapshot.ser");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(currentState); // Serialize the log entries
			out.close();
			fileOut.close();
			System.out.println("Log Snapshot saved successfully");
		} catch (IOException i) {
			i.printStackTrace();
		}
	}
	public void loadFromSnapshot() {
		try {
			FileInputStream fileIn = new FileInputStream("replicatedLogSnapshot.ser");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			List<LogEntry> logEntries = (List<LogEntry>) in.readObject(); // Assuming you have a list of LogEntry objects
			// You might need additional logic here to apply the log entries to the current state
			in.close();
			fileIn.close();
		} catch (IOException | ClassNotFoundException i) {
			i.printStackTrace();
		}
	}
	
}
