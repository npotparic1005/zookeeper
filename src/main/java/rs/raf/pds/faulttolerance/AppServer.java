package rs.raf.pds.faulttolerance;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.LoggerFactory;
import rs.raf.pds.faulttolerance.gRPC.AccountServiceGrpc;
import rs.raf.pds.faulttolerance.gRPC.LogEntry;
import rs.raf.pds.zookeeper.core.SyncPrimitive;

public class AppServer extends SyncPrimitive implements Runnable, ReplicatedLog.LogReplicator{

	public static final String APP_ROOT_NODE ="/account";
	public static final String REPLICA_NODE_NAME ="/candid";
	public static final int REPLICA_NODE_SEQUENCE_INDEX= REPLICA_NODE_NAME.length()-1; 
	public enum Role {FOLLOWER, LEADER};
	
	
	static AccountService accountService;
	int myId = -1;
	volatile Role myRole = Role.FOLLOWER;
	final String myGRPCAddress; 
	Map<String, FollowerGRPCChannel> followersChannelMap = new HashMap<String, FollowerGRPCChannel>();
	String leaaderGRPCAddress = null;
	
	volatile boolean running = false;
	private Thread thread = null;
	 
	protected AppServer(String zkAddress, String zkRoot, String myGRPCAddress) {
		super(zkAddress);
		this.root = zkRoot;
		this.myGRPCAddress = myGRPCAddress;
		
		// Create membership node
        if (zk != null) {
            try {
                Stat s = zk.exists(zkRoot, false);
                if (s == null) {
                    zk.create(zkRoot, new byte[0], Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
                }
               
               // Kreira svoj čvor gde je value hostName:grpcPort kako bi mu pristupao lider
               String myNodeName = zk.create(zkRoot + REPLICA_NODE_NAME, myGRPCAddress.getBytes(), Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL_SEQUENTIAL);
               
               System.out.println("My Node election name:"+myNodeName);
               int tempIndex = myNodeName.indexOf(REPLICA_NODE_NAME)+REPLICA_NODE_NAME.length();
               this.myId = Integer.parseInt(myNodeName.substring(tempIndex));
               
               System.out.println("Node election ID = "+myId);
                
            } catch (KeeperException e) {
                System.out
                        .println("Keeper exception when instantiating queue: "
                                + e.toString());
            } catch (InterruptedException e) {
                System.out.println("Interrupted exception");
            }
        }
	}
	protected void setAccountService(AccountService accountService) {
		this.accountService = accountService;
	}
	protected void setLeader(List<String> nodeList) throws KeeperException, InterruptedException {
		
		myRole = Role.LEADER;
		setFollowersGRPCChannels(nodeList);
		// Lider prati ako se novi čvor povezao u u grupu
		// Da bi ga dodao u svoju listu za replikaciju log-a
		zk.getChildren(root, true);
		
		accountService.setServerState(true);
		System.out.println("JA SAM LIDER!");
	}
	protected void setFollowersGRPCChannels(List<String> nodeList) {
		Map<String, FollowerGRPCChannel> oldMap = followersChannelMap;
		followersChannelMap = new HashMap<String, FollowerGRPCChannel>();
		for (int i=1; i<nodeList.size();i++) {
			String nodeName = nodeList.get(i);
			FollowerGRPCChannel followerChannel = oldMap.get(nodeName);
			try {
				if (followerChannel == null) {
					
					byte[] b = zk.getData(root + "/" + nodeName, false, null);
					String grpcConnection = new String(b);
					String[] tokens = grpcConnection.split(":");
					ManagedChannel channel = ManagedChannelBuilder.forAddress(tokens[0], Integer.parseInt(tokens[1]))
					          .usePlaintext()
					          .build();

					AccountServiceGrpc.AccountServiceBlockingStub blockingStub = AccountServiceGrpc.newBlockingStub(channel);
					followerChannel = new FollowerGRPCChannel(nodeName, grpcConnection, blockingStub);
					
					
				}else
					oldMap.remove(nodeName);
				
				followersChannelMap.put(nodeName, followerChannel);
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
				
		}
	}
	@Override
	public void replicateOnFollowers(Long entryAtIndex, byte[] data) {
		LogEntry logEntry = LogEntry.newBuilder()
							.setEntryAtIndex(entryAtIndex)
							.setLogEntryData(ByteString.copyFrom(data))
							.build();
		for (FollowerGRPCChannel grpcChannel:followersChannelMap.values()) {
			grpcChannel.blockingStub.appendLog(logEntry);
		}
		
	}
	private void checkReplicaCandidate() throws KeeperException, InterruptedException {
		List<String> list = zk.getChildren(root, false);
        System.out.println("There are total:"+list.size()+ " replicas for elections!");
        for (int i=0; i<list.size(); i++) 
        	System.out.print("NODE:"+list.get(i)+", ");
        System.out.println();
        
        if (list.size() == 0) {
            System.out.println("0 Elemenata ? A ja ??? ");
           // mutex.wait();
        } else {
            Collections.sort(list);
            int myIndex = -1;
            
            for(int i=0; i<list.size(); i++) {
            	Integer tempValue = Integer.parseInt(list.get(i).substring(REPLICA_NODE_SEQUENCE_INDEX));
                if(myId == tempValue) {
                   myIndex = i;
                   break;
                }
            }
            if (myIndex == 0) {
            	System.out.println("Priprema za postavku lidera!");
            	setLeader(list);
            }
            else {
            	String totalLeader = list.get(0);
            	byte[] b = zk.getData(root + "/" + totalLeader, false, null);
            	leaaderGRPCAddress = new String(b);
            	
            	String myLeaderNodeToWatch = list.get(myIndex-1);
            	b  = zk.getData(root + "/" + myLeaderNodeToWatch, true, null);
				//Stat stat = zk.exists(root + "/" + myLeaderNodeToWatch, true);
            	//if (stat == null)
            	//	setLeader();
            }
        }
	}


	public void election() throws KeeperException, InterruptedException {
		checkReplicaCandidate();
	}
	
	@Override
	public void run() {
		while(running) {
			synchronized(mutex) {
			   try {
				mutex.wait();
				System.out.println("Stigla notifikacija promene configuracije");
				checkReplicaCandidate();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (KeeperException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			  
			}

		}
		
	}
	public void start() {
		if (!running) {
			thread = new Thread(this, "Node");
			running = true;
			thread.start();
		}
	}
	public void stop() {
		Thread stopThread = thread;
		thread = null;
		running = false;
		stopThread.interrupt();
		
	}
	public boolean isLeader() {
		return myRole == Role.LEADER;
	}
	public String getMyGRPCAddress() {
		
		return myGRPCAddress;
	}
	private AtomicInteger acknowledgmentCounter = new AtomicInteger(0);
	private final int MAJORITY = (followersChannelMap.size() / 2) + 1; // Define majority



	public String getLeaaderGRPCAddress() {
		return leaaderGRPCAddress;
	}
	public static void main(String[] args) throws IOException, InterruptedException {
		ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
		root.setLevel(ch.qos.logback.classic.Level.OFF);

		 if (args.length != 3) {
				System.out.println("Usage java -cp PDS-FT1-1.0.jar;.;lib/* rs.raf.pds.faulttolerance.AppServer <zookeeper_server_host:port> <gRPC_port> <log_file_name>");
			    System.exit(1);
		 }
		
		 String zkConnectionString = args[0];
		 int gRPCPort = Integer.parseInt(args[1]);
		 String logFileName = args[2];
		 
		 String myGRPCaddress = InetAddress.getLocalHost().getHostName()+":"+gRPCPort;
		 AppServer node = new AppServer(zkConnectionString, APP_ROOT_NODE, myGRPCaddress);
		 ReplicatedLog replicatedLog = new ReplicatedLog(logFileName, node);
		 
		 AccountService accService = new AccountService(replicatedLog);
		 node.setAccountService(accService);

		// 1.4 ucitavanje poslednjeg snapshota
		//accountService.loadFromSnapshot();
		//replicatedLog.loadFromSnapshot();
		 
		Server gRPCServer = ServerBuilder
          .forPort(gRPCPort)
          .addService(new AccountServiceGRPCServer(accService, node)).build();

        gRPCServer.start();

		//1.3 snapshot scheduler koji radi u jednakim vremenskim intervalima

		SnapshotScheduler scheduler = new SnapshotScheduler();
		scheduler.startSnapshotRoutine(accService, replicatedLog, 1, TimeUnit.MINUTES);
 
              
        try{
	        node.election();
	        node.start();
	        	        	        
	        gRPCServer.awaitTermination();
	        
	        node.stop();
	        
	     } catch (KeeperException e){

	     } catch (InterruptedException e){

	     }

    }
	

}
