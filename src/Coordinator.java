import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * The class Coordinator manages the Two Phase Commit of file operations.
 * Depending on the file operation, it creates appropriate request and sends it
 * to all participants initiating the first phase of Two Phase Commit. The
 * Coordinator, based on response received from all participants i.e voting,
 * sends final request of Commit or Abort to all Participants. The Coordinator
 * also included Concurrency Control for update to same data structure.It also
 * includes Logging for failure handling and recovery.
 * 
 * @author chetan
 *
 */
public class Coordinator {

	private Coordinator_Handler coordinator_handler;
	private List<Participant> participant_list;
	private List<Transaction> transactions;
	private File participants_file;
	private int coordinator_port;
	private long transaction_id=10000;
	private final Object lock=new Object();
	private final Object transaction_lock=new Object();
	
	public Coordinator() {

	}

	public static void main(String[] args) {

		Coordinator coordinator = new Coordinator();

		if (args.length < 0) 
			System.out.println(Constants.no_args_error);
		
		else {
			
			if(coordinator.parseArguments(args)) {
				
				coordinator.initTransactionList();
				
				coordinator.startServer();	
			}
		}
	}

	private boolean parseArguments(String[] args) {

		boolean flag=false;
		
		try {
			
			coordinator_port = Integer.valueOf(args[0]);

			participants_file = new File(args[1]);

			Scanner input = new Scanner(participants_file);
			
			participant_list=new ArrayList<Participant>();

			while (input.hasNextLine()) {

				String line = input.nextLine();

				line = line.trim();

				String[] details = line.split(" ");

				if (details.length > 1) {

					Participant participant = new Participant();

					participant.name = details[0];

					participant.ip = details[1];

					participant.port = (Integer.parseInt(details[2]));

					participant_list.add(participant);
				}
			}

			if (input != null)
				input.close();
			
			flag=true;
			
		} catch (NumberFormatException nfe) {
			
			flag=false;
			
			System.out.println("Please enter a valid port number !!");

		} catch (FileNotFoundException e) {
			
			flag=false;
			
			System.out.println("Participants file not found !!");
		}
		
		return flag;
	}
	
	
	/**
	 * Starts the server at Co-ordinator to serve incoming RPC requests.
	 */
	private void startServer() {

		new Thread(new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub
				try {

						TServerSocket serverTransport = new TServerSocket(coordinator_port);

						coordinator_handler = new Coordinator_Handler(Coordinator.this, participant_list);

						FileStore.Processor<Coordinator_Handler> processor = new FileStore.Processor<Coordinator_Handler>(
								coordinator_handler);

						// Creates a multi-threaded thrift server to serve multiple requests.						
						TServer server = new TThreadPoolServer(
								new TThreadPoolServer.Args(serverTransport).processor(processor));

						System.out.println("coordinator started and running on port " + coordinator_port);

						server.serve();

				} catch (TTransportException e) {

					e.printStackTrace();
				}
			}
		}).start();
	}
	
	
	/**
	 * Returns new Transaction ID by incrementing current ID by 1. Each
	 * transaction ID corresponds to file operation.
	 * 
	 * @return Transaction ID
	 */
	private long getTran_ID() {
		
		synchronized (lock) {
			
			transaction_id++;
			
			return transaction_id;
		}
	}
	
	/**
	 * This method maintains a permanent log of each transaction which can be
	 * used during recovery.
	 */
	private void serializeTransactions() {
		
		synchronized (transaction_lock) {
			
			try {

				FileOutputStream fout = new FileOutputStream("transactions");

				ObjectOutputStream oos = new ObjectOutputStream(fout);

				oos.writeObject(transactions);

				if (oos != null)
					oos.close();

				if (fout != null)
					fout.close();

			} catch (Exception ex) {

				ex.printStackTrace();
			}
		}
	}
	
	/**
	 * The method initTransactionList initializes the Transaction List using
	 * logs.
	 */
	private void initTransactionList() {
		
		try {
		    
				FileInputStream fileInputStream = new FileInputStream("transactions");
		    
				ObjectInputStream objectinputstream = new ObjectInputStream(fileInputStream);
		    
				transactions=(List<Transaction>) objectinputstream.readObject();
				
				if(objectinputstream!=null)
					objectinputstream.close();
				
				if(fileInputStream!=null)
					fileInputStream.close();
				
				System.out.println("\n--- Transaction Log ---");
				
				for(Transaction trans:transactions) {
					
					transaction_id=trans.getTran_id();	
					
					System.out.println("\ntid "+trans.getTran_id());
					System.out.println("operation "+trans.getOperation_name());
					System.out.println("clientid "+trans.getClient_id());
					System.out.println("filename "+trans.getFile_name());
					System.out.println("status "+trans.getTran_status());
				}
		
		} catch(FileNotFoundException fe) {
			
			System.out.println("Transaction Log File Not Found at Coordinator");
			
		} catch (Exception e) {
		    
			e.printStackTrace();
		} 	
		
		finally {
			
			if(transactions==null)
				transactions=new ArrayList<Transaction>();
		}
	}
	
	/**
	 * Adds a transaction to the Transaction List.
	 * 
	 * @param transaction
	 *            Object of Transaction representing file operation.
	 */
	private void addTransactionToList(Transaction transaction) {
		
		synchronized (transaction_lock) {
			
			if(transactions!=null)
				transactions.add(transaction);
		}
	}
	
	
	/**
	 * Returns Object of Transaction from Transaction List, given required
	 * Transaction ID.
	 * 
	 * @param tran_id
	 *            ID of transaction.
	 * @return Object of Transaction.
	 */
	private Transaction getTransaction(long tran_id) {
		
		synchronized (transaction_lock) {
			
			Transaction transaction=null;
			
			for(Transaction trans:transactions) {
				
				if(trans.getTran_id()==tran_id) {
					
					transaction=trans;
					
					break;
				}
			}
			
			return transaction;
		}
	}
	
	
	/**
	 * Updates Transaction Status to Commit or Abort based on boolean parameter.
	 * 
	 * @param transaction
	 *            Object of Transaction
	 * @param isCommit
	 *            Represents Transaction Status. True for Commit, False for
	 *            Abort.
	 */
	private void updateTransactionInList(Transaction transaction,boolean isCommit) {
		
		synchronized (transaction_lock) {
			
			for(Transaction trans:transactions) {
				
				if(trans.getTran_id()==transaction.getTran_id()) {
					
					if(isCommit)
						trans.setTran_status(T_Status.COMMIT);
					else 
						trans.setTran_status(T_Status.ABORT);
					
					break;
				}
			}
		}
	}
	
	
	/**
	 * The method manageWrite() initiates first phase of Protocol for write
	 * operation. It creates canCommit() request and sends it to all participants.
	 * Based on the response received from each participant, the coordinator
	 * decides to commit or abort the required Transaction.
	 * 
	 * @param rFile
	 *            Object of RFile.
	 * @return True if write at all Participants was successful, false
	 *         otherwise.
	 */
	@SuppressWarnings("finally")
	public boolean manageWrite(RFile rFile) {
		
		int num_votes=0;
		
		boolean flag=false;
		
		Transaction writeTransaction=initTransactionData(rFile);
		
		addTransactionToList(writeTransaction);
		
		serializeTransactions();
		
		try {
				
				for (int i = 0; i < participant_list.size(); i++) {

					Participant participant = participant_list.get(i);

					TTransport clientTransport = new TSocket(participant.getIp(), participant.getPort(), 12000);

					clientTransport.open();

					TProtocol protocol = new TBinaryProtocol(clientTransport);

					Participant_Interface.Client client = new Participant_Interface.Client(protocol);

					StatusReport writeStatus = client.canCommit(writeTransaction);

					if (writeStatus.getStatus() == Status.SUCCESSFUL)
						num_votes++;

					if (clientTransport != null)
						clientTransport.close();

			}
				
		} catch (TException e) {

			// TODO Auto-generated catch block
			System.out.println("Coordinator timeout");
			num_votes = 0;
			// e.printStackTrace();
		}

		finally 
		{
			
			if (num_votes == participant_list.size()) {
				
				flag = true;

				// Initiate second phase of Protocol with Commit.
				secondphase(writeTransaction, true);

			}

			else {
				
				flag = false;

				// Initiate second phase of Protocol with Abort.
				secondphase(writeTransaction, false);
			}

			return flag;
		}
	}
	
	
	/**
	 * Creates and Initializes Transaction Object with necessary parameters.
	 * 
	 * @param rFile
	 *            Object of RFile.
	 * @return Object of newly created Transaction.
	 */
	private Transaction initTransactionData(RFile rFile) {
		
		Transaction transaction=new Transaction();
		
		transaction.setTran_id(getTran_ID());
		
		transaction.setOperation_name("write");
		
		transaction.setClient_id(rFile.getClientID());
		
		transaction.setFile_name(rFile.getFilename());
		
		transaction.setRFile(rFile);
		
		transaction.setTran_status(T_Status.PENDING);
		
		return transaction;
		
	}
	
	
	/**
	 * The method secondphase() includes functionality of second phase of Two
	 * Phase Commit. It updates the transaction status based on boolean
	 * parameter and sends corresponding decision of Commit or Abort to all
	 * Participants.
	 * 
	 * @param transaction
	 *            Object of Transaction representing file operation.
	 * @param isCommit
	 *            Status of Transaction.
	 */
	private void secondphase(Transaction transaction, boolean isCommit) {

		updateTransactionInList(transaction, isCommit);
		
		serializeTransactions();
		
		try {
			
			for (int i = 0; i < participant_list.size(); i++) {

				Participant participant = participant_list.get(i);
				
				TTransport clientTransport = new TSocket(participant.getIp(), participant.getPort(),12000);

				clientTransport.open();

				TProtocol protocol = new TBinaryProtocol(clientTransport);

				Participant_Interface.Client client = new Participant_Interface.Client(protocol);
				
				if (isCommit)
					client.doCommit(transaction.getTran_id());
				else
					client.doAbort(transaction.getTran_id());

				if (clientTransport != null)
					clientTransport.close();

			}
			
		} catch(TTransportException tte) {
			
			System.out.println(tte.getMessage());
		
		}catch (TException e) {

			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * The method manageDelete() initiates first phase of Protocol for delete
	 * operation. It creates canCommit() request and sends it to all
	 * participants. Based on the response received from each participant, the
	 * coordinator decides to commit or abort the required Transaction.
	 * 
	 * @param filename
	 *            File name to delete
	 * @param clientID
	 *            Client ID representing owner of File.
	 * @return True if delete at all Participants was successful, false
	 *         otherwise.
	 */
	public boolean manageDelete(String filename,String clientID) {
		
		boolean deleteFlag=false;
		
		int num_votes=0;
		
		Transaction deleteTransaction=new Transaction();
		
		deleteTransaction.setTran_id(getTran_ID());
		
		deleteTransaction.setOperation_name("delete");
		
		deleteTransaction.setClient_id(clientID);
		
		deleteTransaction.setFile_name(filename);
		
		deleteTransaction.setTran_status(T_Status.PENDING);
			
		addTransactionToList(deleteTransaction);
		
		serializeTransactions();
		
		for(int i=0;i<participant_list.size();i++) {
			
			Participant participant=participant_list.get(i);
			
			try {

					TTransport clientTransport = new TSocket(participant.getIp(),participant.getPort());

					clientTransport.open();

					TProtocol protocol = new TBinaryProtocol(clientTransport);

					Participant_Interface.Client client=new Participant_Interface.Client(protocol);
					
					StatusReport writeStatus=client.canCommit(deleteTransaction);
					
					if(writeStatus.getStatus()==Status.SUCCESSFUL)
						num_votes++;

					if(clientTransport!=null)
						clientTransport.close();
					
			} catch (TException e) {
					
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		if(num_votes==participant_list.size()) {
			
			deleteFlag=true;
			
			secondphase(deleteTransaction,true);
			
		}
		
		else {
			
			deleteFlag=false;
			
			secondphase(deleteTransaction, false);
		}
	
		return deleteFlag;
	}
	
	
	/**
	 * The method manageRead() initiates first phase of Protocol for read
	 * operation. It reads a file from a random Participant.
	 * 
	 * @param filename
	 *            File name to read.
	 * @param clientID
	 *            Client ID representing owner of File.
	 * @return Object of RFile including File Content, File Name and Owner of
	 *         File.
	 * 
	 */
	public RFile manageRead(String filename,String clientId) {
		
		boolean isCommit=false;
		
		Transaction readTransaction=new Transaction();
		
		readTransaction.setTran_id(getTran_ID());
		
		readTransaction.setOperation_name("read");
		
		readTransaction.setClient_id(clientId);
		
		readTransaction.setFile_name(filename);
		
		readTransaction.setTran_status(T_Status.PENDING);
		
		addTransactionToList(readTransaction);
		
		serializeTransactions();
		
		RFile rFile=new RFile();
		
		int index=new Random().nextInt(participant_list.size());
		
		Participant participant=participant_list.get(index);

		try {
			
				TTransport clientTransport = new TSocket(participant.getIp(),participant.getPort());
			
				clientTransport.open();
			
				TProtocol protocol = new TBinaryProtocol(clientTransport);

				Participant_Interface.Client client=new Participant_Interface.Client(protocol);
			
				rFile=client.readFile(readTransaction);
			
				if(rFile.getContent()!=null)
					isCommit=true;
				
				if(clientTransport!=null)
					clientTransport.close();
		} catch (TTransportException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			
		} catch (SystemException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			
		} catch (TException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			
		}
		
		finally {
			
			updateTransactionInList(readTransaction, isCommit);
			
			serializeTransactions();
			
			return rFile;
		}
	}
	
	/**
	 * The method manageParticipantRequest() handles Transaction status in case
	 * of Participant failure after first phase. The Coordinator retrieves the
	 * status of Transaction from log and accordingly calls Commit() or Abort()
	 * of participant.
	 * 
	 * @param transaction_id
	 *            Transaction ID to verify status.
	 * @param p_ip
	 *            IP address of Participant.
	 * @param p_port
	 *            Port Number of Participant.
	 */
	public void manageParticipantRequest(long transaction_id,String p_ip, int p_port) {
		
		boolean isCommit=false;
		
		Transaction transaction=getTransaction(transaction_id);
		
		if(transaction.getTran_status()==T_Status.COMMIT)
			isCommit=true;
		else if(transaction.getTran_status()==T_Status.ABORT)
			isCommit=false;
		
		try {
			
			TTransport clientTransport = new TSocket(p_ip,p_port);
		
			clientTransport.open();
		
			TProtocol protocol = new TBinaryProtocol(clientTransport);

			Participant_Interface.Client client=new Participant_Interface.Client(protocol);
		
			if (isCommit)
				client.doCommit(transaction.getTran_id());
			else
				client.doAbort(transaction.getTran_id());
		
			if(clientTransport!=null)
				clientTransport.close();
		} catch (TTransportException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SystemException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
