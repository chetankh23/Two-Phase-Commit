import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

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
 * The class LocalParticipant serves incoming RPC requests from Coordinator. It
 * also includes logging mechanism for failure handling and recovery.
 * 
 * @author chetan
 *
 */
public class Local_Participant {

	private String my_id;
	public int my_port;
	private Participant_Handler my_handler;
	private final Object transaction_lock=new Object();
	private List<Transaction> transactions;
	private List<Transaction> pendingTransactions;
	public String coordinator_ip;
	public int coordinator_port;
	
	public Local_Participant() {
		
	}
	
	public static void main(String[] args) {
		
		Local_Participant participant=new Local_Participant();
		
		participant.getFileList();
		
		if(participant.parseArguments(args)) {
			
			participant.initTransactionList();
			
			participant.startServer();
			
			participant.runRecovery();
		}
	}
	
	private void getFileList() {
		
		
	}
	
	private boolean parseArguments(String[] args) {
		
		boolean flag=false;
		
		if(args.length<0)
			System.out.println(Constants.no_args_error);
		
		else {
				try {
				
					this.my_id=args[0];
			
					this.my_port=Integer.parseInt(args[1]);
					
					flag=true;
				
				}catch(NumberFormatException nfe) {
					
					System.out.println(Constants.invalid_port);
				}
		}
		
		return flag;
		
	}
	
	/**
	 * Starts the server at Participant to serve incoming RPC requests.
	 */
	private void startServer() {
		
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				// TODO Auto-generated method stub
				try {

					TServerSocket serverTransport = new TServerSocket(my_port);

					my_handler=new Participant_Handler(Local_Participant.this);
				
					Participant_Interface.Processor<Participant_Handler> processor = new Participant_Interface.Processor<Participant_Handler>(my_handler);
					
					// Creates a multi-threaded thrift server to serve multiple requests.
					TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));

					System.out.println("Participant started and running on port "+my_port);

					server.serve();
				
				} catch (TTransportException e) {

					e.printStackTrace();
				}
			}
		}).start();
		
		
	}
	
	/**
	 * The method runRecovery() includes implementation for recovery purpose
	 * when Participant comes back after failure. It first verifies the pending
	 * transactions from log and calls Coordinator to retrieve the status of
	 * each pending transaction.
	 */
	private void runRecovery() {
		
		System.out.println("Inside run recovery");
		
		getPendingTransactions();
		
		if (pendingTransactions.size() > 0) {

			try {
				
				System.out.println("Enter the host name of the coordinator : ");

				BufferedReader buffer = new BufferedReader(new InputStreamReader(System.in));

				coordinator_ip = buffer.readLine();

				System.out.println("Enter the port number of the coordinator : ");

				coordinator_port = Integer.parseInt(buffer.readLine());
				
				for (Transaction trans : pendingTransactions) {

					if(trans.getOperation_name().equals("write")){
						
						my_handler.canWriteCommit(trans.getRFile());
						
					} else if(trans.getOperation_name().equals("delete")) {
					
						my_handler.canDeleteCommit(trans.getFile_name());
					}
					
					TTransport clientTransport = new TSocket(coordinator_ip, coordinator_port);

					clientTransport.open();

					TProtocol protocol = new TBinaryProtocol(clientTransport);

					FileStore.Client client = new FileStore.Client(protocol);

					client.getTransactionStatus(trans.getTran_id(), my_id, my_port);

					if (clientTransport != null)
						clientTransport.close();
				}
			} catch (NumberFormatException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
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
	
	/**
	 * Verifies status of each Transaction from Log and creates a list of
	 * Pending Transactions.
	 */
	private void getPendingTransactions() {
		
		synchronized (transaction_lock) {
			
			for(Transaction trans:transactions) {
				
				if(trans.getTran_status()==T_Status.PENDING) {
					
					pendingTransactions.add(trans);
				}
			}
		}
	}
	
	
	public void addTransactionToList(Transaction transaction) {
		
		synchronized (transaction_lock) {
			
			if(transactions!=null){
				
				transactions.add(transaction);
			}
		}
	}
	
	/**
	 * Updates the Transaction Status of corresponding Transaction.
	 * @param tran_id Transaction ID.
	 * @param isCommit Status of Transaction, True to Commit, false otherwise.
	 */
	public void updateTransactionInList(long tran_id,boolean isCommit) {
		
		synchronized (transaction_lock) {
			
			for(Transaction trans:transactions) {
				
				if(trans.getTran_id()==tran_id) {
					
					if(isCommit)
						trans.setTran_status(T_Status.COMMIT);
					else 
						trans.setTran_status(T_Status.ABORT);
					
					break;
					
				}
			}
		}
	}
	
	
	public Transaction getTransactionInfo(long tran_id) {
		
		synchronized (transaction_lock) {
			
			for(Transaction trans:transactions) {
				
				if(trans.getTran_id()==tran_id) 
					return trans;
				
			}
			return null;
		}
	}
	
	/**
	 * This method maintains a permanent log of each transaction which can be
	 * used during recovery.
	 */
	public void serializeTransactions() {
		
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
				
				System.out.println("\n --- Transaction Log ---");
				
				for(Transaction trans:transactions) {
		
					System.out.println("\ntid "+trans.getTran_id());
					System.out.println("operation "+trans.getOperation_name());
					System.out.println("clientid "+trans.getClient_id());
					System.out.println("filename "+trans.getFile_name());
					System.out.println("status "+trans.getTran_status());
				}
		
		} catch(FileNotFoundException fe) {
			
			System.out.println("Transaction Log File Not Found at Participant");
			
		} catch (Exception e) {
		    
			e.printStackTrace();
		} 	
		
		finally {
			
			if(transactions==null)
				transactions=new ArrayList<Transaction>();
			
			pendingTransactions=new ArrayList<Transaction>();
		}
	}
}
