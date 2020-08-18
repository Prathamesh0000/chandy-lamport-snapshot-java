import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class Controller {

	Bank.InitBranch.Builder branch = Bank.InitBranch.newBuilder();
	static Map<String, Socket> connections = new HashMap<>();

	public static void main(String[] args) {
		if(args.length != 2){
			System.out.println("Error");
			System.exit(0);
		}
		try {
			int amount = Integer.valueOf(args[0]);
			String name = args[1];
			Controller controller = new Controller();
			controller.parseFileAndInitializeBranches(amount, name);
			controller.sendInitSnapshotMessageToRandomBranch();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void sendInitSnapshotMessageToRandomBranch() {
		Thread initSnapshotThread = new Thread(){
			public void run() {
				Socket socket = null;
				int snapshotId = 1;
				OutputStream outputStream = null;
				Bank.BranchMessage.Builder branchMesssageBuilder = null;
				Bank.InitSnapshot.Builder initSnapshotBuilder = null;
				Bank.RetrieveSnapshot.Builder retrieveSnapshotBuilder = null;
				
				while(true) {
					try {
						Thread.sleep(4000L);
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
					
					int randomThread = ThreadLocalRandom.current().nextInt(0, branch.getAllBranchesCount());
					String ip = branch.getAllBranches(randomThread).getIp();
					int port = branch.getAllBranches(randomThread).getPort();
					String name = branch.getAllBranches(randomThread).getName();
					initSnapshotBuilder = Bank.InitSnapshot.newBuilder();
					initSnapshotBuilder.setSnapshotId(snapshotId);
					branchMesssageBuilder  = Bank.BranchMessage.newBuilder();
					branchMesssageBuilder.setInitSnapshot(initSnapshotBuilder);
					try {
						socket = Controller.connections.get(name);
						outputStream = socket.getOutputStream();
						branchMesssageBuilder.build().writeDelimitedTo(outputStream);
						try {
							Thread.sleep(1000L);
						} catch (InterruptedException e1) {
							e1.printStackTrace();
						}
						branchMesssageBuilder  = Bank.BranchMessage.newBuilder();
						retrieveSnapshotBuilder = Bank.RetrieveSnapshot.newBuilder();
						retrieveSnapshotBuilder.setSnapshotId(snapshotId);
						branchMesssageBuilder.setRetrieveSnapshot(retrieveSnapshotBuilder);
						for(Bank.InitBranch.Branch branchEach : branch.getAllBranchesList()) {
							socket = Controller.connections.get(branchEach.getName());
							outputStream = socket.getOutputStream();
							branchMesssageBuilder.build().writeDelimitedTo(outputStream);
							new ControllerRetrieveSnapshotHandler(socket, branchEach.getName()).start();
						}
						
					} catch (UnknownHostException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
					snapshotId = snapshotId + 1;
				}
			}
		};
		
		initSnapshotThread.start();
	}
	
	private class ControllerRetrieveSnapshotHandler extends Thread {
        private Socket clientSocket;
        private String receiveBranch;
        
        public ControllerRetrieveSnapshotHandler(Socket socket, String name) {
            clientSocket = socket;
            receiveBranch = name;
        }
 
        public void run() {
        	try {
        		InputStream inputStream = clientSocket.getInputStream();
        		Bank.BranchMessage branchMessage = Bank.BranchMessage.parseDelimitedFrom(inputStream);
        		if(branchMessage.hasReturnSnapshot()) {
        			List<Integer> list = branchMessage.getReturnSnapshot().getLocalSnapshot().getChannelStateList();
        			List<String> branchList = new ArrayList<>();
        			for(int i=0; i<branch.getAllBranchesCount(); i++) {
        				if(!branch.getAllBranches(i).getName().equals(receiveBranch)) {
        					branchList.add(branch.getAllBranches(i).getName());
        				}
        			}
        		
        			if(list.size() == branchList.size()) {
						String outStr = "\n";
						outStr += "----------------------------------------------------------------------------------------------------\n";
						outStr += "Snapshot Id:\t" + branchMessage.getReturnSnapshot().getLocalSnapshot().getSnapshotId() + "\n";
						outStr += "Snapshot at:\t" + receiveBranch + "\n";
						outStr += "Snapshot amount at current branch:\t" + branchMessage.getReturnSnapshot().getLocalSnapshot().getBalance() + "\n";

						outStr += "Communication channel Snapshot" +"\n";
        				
        				for(int j=0; j< branchList.size(); j++) {
								outStr +=branchList.get(j) + "->" + receiveBranch + ": " + list.get(j) + "\n" ;
        				}
						outStr += "----------------------------------------------------------------------------------------------------\n";

						System.out.println(outStr);
        			}else {
        				System.out.println("Snapshot pending for snapshot:" + branchMessage.getReturnSnapshot().getLocalSnapshot().getSnapshotId() + " Branch: " + receiveBranch );
        			}
        		}
			} catch (IOException e) {
				e.printStackTrace();
			}
			
        }
    }
	
	private void parseFileAndInitializeBranches(int amount, String fileName) {
		File file = new File(fileName);
		BufferedReader bufferedReader = null;
		if(!file.exists()) {
			System.out.println("Error in file name or address " + fileName);
			System.exit(0);
		}
		
		try {
			bufferedReader = new BufferedReader(new FileReader(file));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.out.print("File not found..!");
			System.exit(0);
		}
		
		int branchCount = 0;
		String line = "";
		try {
			while((line = bufferedReader.readLine()) != null){
				String arr[] = line.split(" ");
				String branchName = arr[0];
				String ipAddress = arr[1];
				int port = Integer.parseInt(arr[2]);
				
				System.out.println(branchName + " " + ipAddress + " " + port);
				Bank.InitBranch.Branch.Builder branchNew = Bank.InitBranch.Branch.newBuilder();
				branchNew.setName(branchName);
				branchNew.setIp(ipAddress);
				branchNew.setPort(port);
				
				branch.addAllBranches(branchNew);
				branchCount++;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		int branchInitAmountt = amount/branchCount;
		branch.setBalance(branchInitAmountt);
		Bank.BranchMessage.Builder branchMessageBuilder  = Bank.BranchMessage.newBuilder();
		branchMessageBuilder.setInitBranch(branch);
		
		Socket clientSocket = null;
		for(Bank.InitBranch.Branch branchEach : branch.getAllBranchesList()) {
			try {
				clientSocket = new Socket(branchEach.getIp(), branchEach.getPort());
				OutputStream outputStream = clientSocket.getOutputStream();
				branchMessageBuilder.build().writeDelimitedTo(outputStream);
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			connections.put(branchEach.getName(), clientSocket);
		}
	}

}
