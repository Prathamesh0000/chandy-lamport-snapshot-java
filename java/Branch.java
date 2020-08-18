import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

public class Branch {
	static int balance = 0;
	int initBalance = 0;	
	static String ip;
	static String branchName;
	static int branchPort;
	Bank.BranchMessage branchDetails;
	static Map<String, Socket> map = new HashMap<>();
	static int isEstablished = 0;
	static int isBranchMessage = 0;
	private final Object lock = new Object();
	private ConcurrentHashMap<Integer, Integer> state = new ConcurrentHashMap<>();
	private Map<Integer, Map<String, Integer>> inState = new HashMap<>();
	private ConcurrentHashMap<Integer, Map<String, Integer>> lastState = new ConcurrentHashMap<>();
	
	public static void main(String[] args) {
		if(args.length != 2){
			System.out.println("Error");
			System.exit(0);
		}
		ServerSocket serverSocket = null;
		Branch branchServer = new Branch();
		try {
			ip = InetAddress.getLocalHost().getHostAddress();
			branchName = args[0];
			branchPort = Integer.valueOf(args[1]);
			serverSocket = new ServerSocket(branchPort);
			System.out.println(branchName+ " Started on " + ip + " " + branchPort );
			Bank.BranchMessage branchMessage = null;
			Socket socket = serverSocket.accept();
			InputStream inputStream = socket.getInputStream();
			branchMessage = Bank.BranchMessage.parseDelimitedFrom(inputStream);
			if(branchMessage.hasInitBranch()) {
				branchServer.initializeBranchDetails(branchMessage);
				branchServer.setUpTCPConnections();
			}
			new ControllerHandler(socket, branchServer).start();
		}catch (IOException e) {
			e.printStackTrace();
		}
		while(true) {
			try {
					Socket socket = serverSocket.accept();
					if(isBranchMessage == 0) {
						BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
						String name = input.readLine();
						map.put(name, socket);
						isEstablished+=1;
						new BranchHandler(socket,branchServer, name).start();
						branchServer.syncTCPConnectionsAndStartAmountTransfer();	
					}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void setUpTCPConnections() {
		int i = 0;
		for(i = 0; i < branchDetails.getInitBranch().getAllBranchesCount(); i++) {
			if(branchDetails.getInitBranch().getAllBranches(i).getName().equals(branchName)) {
				i++;
				break;
			}
		}
		Socket socket = null;
		
		for(int j = i; j < branchDetails.getInitBranch().getAllBranchesCount(); j++) {
			String ipAddress = branchDetails.getInitBranch().getAllBranches(j).getIp();
			int port = branchDetails.getInitBranch().getAllBranches(j).getPort();
			String name = branchDetails.getInitBranch().getAllBranches(j).getName();
			try {
				socket = new Socket(ipAddress, port);
				PrintWriter output = new PrintWriter(socket.getOutputStream(), true);
				output.println(branchName);
			} catch (IOException e) {
				e.printStackTrace();
			}
			map.put(name, socket);
			isEstablished += 1;
			new BranchHandler(socket, this, name).start();
			syncTCPConnectionsAndStartAmountTransfer();
		}
	}
	
	private static class ControllerHandler extends Thread {
        private Socket controllerSocket;
        private Branch branchServer;
        public ControllerHandler(Socket socket, Branch server) {
        	controllerSocket = socket;
        	branchServer = server;
        }
        public void run() {
        	try {
        		InputStream inputStream = controllerSocket.getInputStream();
        		Bank.BranchMessage.Builder branchMessageBuilder  = Bank.BranchMessage.newBuilder();
        		Bank.BranchMessage branchMessage = null;
        		while( (branchMessage = Bank.BranchMessage.parseDelimitedFrom(inputStream)) != null){
        			String branchName = branchMessage.getTransfer().getSendBranch();
        			if(branchMessage.hasInitSnapshot()) {
        				branchServer.initiateLocalSnapshotProcedure(branchMessage.getInitSnapshot().getSnapshotId(), branchName);
        			}
        			if(branchMessage.hasRetrieveSnapshot()) {
        				Bank.ReturnSnapshot returnSnapshot = branchServer.returnSnapshotToController(branchMessage.getRetrieveSnapshot().getSnapshotId());
        				branchMessageBuilder  = Bank.BranchMessage.newBuilder();
        				branchMessageBuilder.setReturnSnapshot(returnSnapshot);
        				branchMessageBuilder.build().writeDelimitedTo(controllerSocket.getOutputStream());
        			}
        		}
			} catch (IOException e) {
				e.printStackTrace();
			}
        }
    }
	
	private static class BranchHandler extends Thread {
        private Socket clientSocket;
        private Branch branchServer;
        private String fromBranch;
        public BranchHandler(Socket socket, Branch server, String name) {
            clientSocket = socket;
            branchServer = server;
            fromBranch = name;
        }
        public void run() {
        	try {
        		InputStream inputStream = clientSocket.getInputStream();
        		Bank.BranchMessage branchMessage = null;
        		while( (branchMessage = Bank.BranchMessage.parseDelimitedFrom(inputStream)) != null){
        			if(branchMessage.hasTransfer()) {
						int amount = branchMessage.getTransfer().getAmount();
						branchServer.updateBalance(amount);
						branchServer.updateAmountForAllRecordingChannels(amount, fromBranch);
					}
        			if(branchMessage.hasMarker()) {
						// Simulate slow recieving of marker msg
						long sleep = (long)(Math.random()*(1000));
						try {
							Thread.sleep(sleep);
						} catch (InterruptedException e1) {
							e1.printStackTrace();
						}
        				branchServer.receiveMarkerMessage(branchMessage.getMarker().getSnapshotId(), fromBranch);
        			}
        		}
			} catch (IOException e) {
				e.printStackTrace();
			}
        }
    }
    
	public void syncTCPConnectionsAndStartAmountTransfer() {
		int branchCount = branchDetails.getInitBranch().getAllBranchesCount()-1;
		if(branchCount == isEstablished) {
			isBranchMessage = 1;
			transferAmountToAllBranches();
		}
	}
	
	private void initiateLocalSnapshotProcedure(int snapshotId, String branchName) {
		recordLocalState(snapshotId);
		createMapForIncomingChannels(snapshotId);
		Bank.Marker.Builder marker = Bank.Marker.newBuilder();
		marker.setSnapshotId(snapshotId);
		marker.setSendBranch(branchName);
		sendMarkerMessagesToOtherBranches(marker);
	}
	
	private void sendMarkerMessagesToOtherBranches(Bank.Marker.Builder marker) {
		Bank.BranchMessage.Builder branchMesssageBuilder  = Bank.BranchMessage.newBuilder();
		branchMesssageBuilder.setMarker(marker);
		Iterator iterator = map.entrySet().iterator();
	    while (iterator.hasNext()) {
	        Map.Entry pair = (Map.Entry)iterator.next();
	        	Socket socket = map.get(pair.getKey());
	        	OutputStream outputStream;
				try {
					outputStream = socket.getOutputStream();
					branchMesssageBuilder.build().writeDelimitedTo(outputStream);
				} catch (IOException e) {
					e.printStackTrace();
				}
	    }
	}
	
	private void receiveMarkerMessage(int snapshotId, String fromBranch) {
		if(inState.get(snapshotId) != null){
			recordFinalChannelStateAndStopRecording(snapshotId,fromBranch);
		}else {
			recordLocalState(snapshotId);
			createMapForIncomingChannels(snapshotId);
			inState.get(snapshotId).put(fromBranch, 0);
			recordFinalChannelStateAndStopRecording(snapshotId,fromBranch);
			Bank.Marker.Builder marker = Bank.Marker.newBuilder();
			marker.setSnapshotId(snapshotId);
			marker.setSendBranch(fromBranch);
			sendMarkerMessagesToOtherBranches(marker);
		}
	}
	
	private void recordFinalChannelStateAndStopRecording(int snapshotId, String fromBranch) {
		Map<String, Integer> hashMap; 
		if(lastState.get(snapshotId) == null) {
			hashMap = new HashMap<>();
			lastState.put(snapshotId, hashMap);
		}
		if(inState.get(snapshotId) != null) {
			int recordedBalance = inState.get(snapshotId).get(fromBranch);
			lastState.get(snapshotId).put(fromBranch, recordedBalance);
		}
	}
	
	private void updateAmountForAllRecordingChannels(int amount, String fromBranch) {
		Iterator iterator = inState.entrySet().iterator();
	    while (iterator.hasNext()) {
	        Map.Entry entry = (Map.Entry)iterator.next();
	        Map<String, Integer> hashMap = new HashMap<>();
	        hashMap = inState.get(entry.getKey());
	        if(hashMap.get(fromBranch) != null) {
	        	int updatedBal;
	        	if(hashMap.get(fromBranch) == -1)
	        		updatedBal = amount;
	        	else
		        	updatedBal = amount + hashMap.get(fromBranch);
	        	hashMap.put(fromBranch, updatedBal);
	        	inState.put((Integer) entry.getKey(), hashMap);
	        }
	    }
		
	}
	
	private void createMapForIncomingChannels(int snapshotId) {
		if(inState.get(snapshotId) == null) {
			Map<String, Integer> hashMap = getInitializeMap();
			inState.put(snapshotId, hashMap);
		}
	}
	
	private void recordLocalState(int snapshotId) { state.put(snapshotId, balance); }
	
	private Map<String, Integer> getInitializeMap(){
		Map<String, Integer> hashMap = new HashMap<>();

		Iterator iterator = map.entrySet().iterator();
	    while (iterator.hasNext()) {
	        Map.Entry pair = (Map.Entry)iterator.next();
	        hashMap.put((String) pair.getKey(), -1);
	    }
	    return hashMap;
	}
	
	private void sleepForOneSecond() {
		try{
			Thread.sleep(100L);
		}catch(InterruptedException e3){
			e3.printStackTrace();
		}	
	}
	
	private Bank.ReturnSnapshot returnSnapshotToController(int snapshotId) {
		Bank.ReturnSnapshot.Builder result = Bank.ReturnSnapshot.newBuilder();
		
		Bank.ReturnSnapshot.LocalSnapshot.Builder local = Bank.ReturnSnapshot.LocalSnapshot.newBuilder();
		local.setSnapshotId(snapshotId);
		while(lastState.get(snapshotId) == null) { 
			sleepForOneSecond();
		}	
		while(lastState.get(snapshotId).size() != map.size()){
			sleepForOneSecond();
		}

		local.setBalance(state.get(snapshotId));
		Map<String, Integer> channelMap = lastState.get(snapshotId);
		List<Integer> list = new ArrayList<>();
		for(Bank.InitBranch.Branch branch : branchDetails.getInitBranch().getAllBranchesList()) {
			if(channelMap.get(branch.getName()) != null) {
				if(channelMap.get(branch.getName()) == -1)
					list.add(0);
				else
					list.add(channelMap.get(branch.getName()));
			}
		}
		
		local.addAllChannelState(list);
		
		result.setLocalSnapshot(local);
		
		return result.build();
	}
	
	private void initializeBranchDetails(Bank.BranchMessage branchMsg) {
		initBalance = branchMsg.getInitBranch().getBalance();
		balance = initBalance;
		branchDetails = branchMsg;
		System.out.println(branchName + " initial amount:: " + balance);
	}
	
	private String getRandomBranchName() {
		String name = null;
		int totalBranches = branchDetails.getInitBranch().getAllBranchesCount();
		int index = ThreadLocalRandom.current().nextInt(0, totalBranches);
		while(branchDetails.getInitBranch().getAllBranches(index).getName().equals(branchName)) {
			index = ThreadLocalRandom.current().nextInt(0, totalBranches);
		}
		name = branchDetails.getInitBranch().getAllBranches(index).getName();
		return name;
	}
	
	private int getAmountToTransfer() {
		int amount = 0;
		int low = (int) (0.01*initBalance);
		int high = (int) (0.05*initBalance);
		amount = ThreadLocalRandom.current().nextInt(low, high+1);
		System.out.println();
		synchronized (lock) {
			if((balance - amount) > 0) {
				System.out.println("Before Transfer:: " + balance);
					balance = balance - amount;
				System.out.println("After Transfer:: " + balance);
			} else
				amount = 0;
		}
		return amount;
	}
	
	private void updateBalance(int amount){
		synchronized (lock) {
			System.out.print("Received(" + balance + "+" + amount + ")");
			balance = balance + amount;
		}
		System.out.print(" = " + balance);
		System.out.println();
	}
	
	private void transferAmountToAllBranches() {
		Thread sendAmount = new Thread() {
			public void run() {
				while(true){
					long sleep = (long)(Math.random()*(1000));
					try {
						Thread.sleep(sleep);
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
					String branch = getRandomBranchName();
					Socket clientSocket;
					try {
						clientSocket = map.get(branch);
						Bank.Transfer.Builder message = Bank.Transfer.newBuilder();
						int transferAmount = getAmountToTransfer();
						if(transferAmount > 0) {
							message.setAmount(transferAmount);
							message.setSendBranch(branch);
							Bank.BranchMessage.Builder branchMessageBuilder  = Bank.BranchMessage.newBuilder();
							branchMessageBuilder.setTransfer(message);
							System.out.println("Transfering : " + branchName + "->" + branch + " || Amt:  " + transferAmount);
							branchMessageBuilder.build().writeDelimitedTo(clientSocket.getOutputStream());
						}
						
					} catch (UnknownHostException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		};
		
		sendAmount.start();
	}
		
}