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
		fs = new FileOutputStream(fileName,true);
		writer = new OutputStreamWriter(fs);
	}
		
	public void appendAndReplicate(byte[] data) throws IOException {
		//Long lastLogEntryIndex = appendToLocalLog(data);
		appendToLocalLog(data);
		node.replicateOnFollowers(lastLogEntryIndex, data);  
	}
	
	protected void appendToLocalLog(byte[] data) throws IOException {
		String s = new String(data);
		System.out.println("Log #"+lastLogEntryIndex+":"+s);
		
		//fs.write(data);
		//fs.flush();
		writer.write(s);writer.write("\r\n");
		writer.flush();
		//fs.flush();

		lastLogEntryIndex++;
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

	public synchronized void takeSnapshot() {
		try {
			FileOutputStream snapshotOutputStream = new FileOutputStream("replicatedLogSnapshot.ser");
			ObjectOutputStream objectOutputStream = new ObjectOutputStream(snapshotOutputStream);
			objectOutputStream.writeObject(getLastLogEntryIndex());
			objectOutputStream.close();
			snapshotOutputStream.close();
			System.out.println("Snapshot taken at log entry index: " + lastLogEntryIndex);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void loadFromSnapshot() {
		try {
			FileInputStream fileIn = new FileInputStream("replicatedLogSnapshot.ser");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			this.lastLogEntryIndex = (Long) in.readObject();
			System.out.println("Loaded last snapshot at log entry index: " + lastLogEntryIndex);
			in.close();
			fileIn.close();
		} catch (IOException | ClassNotFoundException i) {
			i.printStackTrace();
		}

//		try {
//			FileInputStream fileIn = new FileInputStream("replicatedLogSnapshot.ser");
//			// FileInputStream fileIn = new FileInputStream("replicatedLogSnapshot.txt");
//			ObjectInputStream in = new ObjectInputStream(fileIn);
//			Object object;
//
//			Long snapshotLastLogEntryIndex = null;
//			while ((object = in.readObject()) != null) {
//				if (object instanceof Long) {
//					snapshotLastLogEntryIndex = (Long) object;
//
//					this.lastLogEntryIndex = snapshotLastLogEntryIndex; // Postavljanje učitane vrednosti kao poslednji indeks loga
//					System.out.println("Loaded last snapshot at log entry index: " + lastLogEntryIndex);
//
//					in.close();
//					fileIn.close();
//				}
//			}
//
//		} catch (EOFException e) {
//			System.out.println("Reached end of file while reading snapshot.");
//			e.printStackTrace();
//		} catch (IOException | ClassNotFoundException e) {
//			e.printStackTrace();
//		}
	}

}
