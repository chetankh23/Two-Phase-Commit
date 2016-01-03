import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.List;
import org.apache.thrift.TException;

/**
 * The class Participant_Handler manages incoming RPC requests from Coordinator.
 * It also includes Concurrency Control to handle multiple requests for the same
 * file.
 * 
 * @author chetan
 *
 */
public class Participant_Handler implements Participant_Interface.Iface{

	private Local_Participant local_participant;
	private List<String> fileList=new ArrayList<String>();
	private List<PendingFile> filesToDelete,filesToWrite;
	private final Object delete_lock_object=new Object();
	private final Object write_lock_object=new Object();
	
	public Participant_Handler(Local_Participant local_participant) {
		// TODO Auto-generated constructor stub
		
		this.local_participant=local_participant;
		
		filesToDelete=new ArrayList<PendingFile>();
		
		filesToWrite=new ArrayList<PendingFile>();
	}
	
	/**
	 * Handles first phase of Protocol. It Participant is alive and meets
	 * necessary conditions to perform desired operation, Transaction Status is
	 * updated to Pending, else it Aborts. 
	 */
	@Override
	public StatusReport canCommit(Transaction tran_info) throws SystemException, TException {
		// TODO Auto-generated method stub
		
		StatusReport status_report=new StatusReport();
		
		String operationName=tran_info.getOperation_name();
		
		if(operationName.equals("write")) {
			
			try {
				
				RFile rFile=tran_info.getRFile();
				
				if(canWriteCommit(rFile)) {
			
					status_report.status=Status.SUCCESSFUL;
				
					tran_info.setTran_status(T_Status.PENDING);
				}
				else {
			
					status_report.status=Status.FAILED;
				
					tran_info.setTran_status(T_Status.ABORT);
				}
				
			} catch(Exception e) {
				
				status_report.status=Status.FAILED;
				
				tran_info.setTran_status(T_Status.ABORT);
			}
			
			finally{
				
				local_participant.addTransactionToList(tran_info);
				
				local_participant.serializeTransactions();
				
			}
		}
		
		else if(operationName.equals("delete")) {
			
			String filename=tran_info.getFile_name();
			
			try {
				
				if (canDeleteCommit(filename)) {

					status_report.status = Status.SUCCESSFUL;
					
					tran_info.setTran_status(T_Status.PENDING);
				}

				else {

					status_report.status = Status.FAILED;
					
					tran_info.setTran_status(T_Status.ABORT);
				}
				
			} catch (Exception e) {
				
				System.out.println("Exception for delete at can commit is "+e.getMessage());
			}
			
			finally {
				
				local_participant.addTransactionToList(tran_info);
				
				local_participant.serializeTransactions();
			}
		}
		
		return status_report;
	}	

	
	/**
	 * Commits the transaction with given transaction ID.
	 */
	@Override
	public void doCommit(long tran_id) throws SystemException, TException {
		// TODO Auto-generated method stub
		
		Transaction transaction=local_participant.getTransactionInfo(tran_id);
		
		if(transaction!=null) {
		
			local_participant.updateTransactionInList(tran_id, true);
			
			local_participant.serializeTransactions();
			
			if(transaction.operation_name.equals("delete")) {
			
				PendingFile d_file=getFileToDelete(transaction.getFile_name());
		
				if(d_file!=null) {
					
					deleteFile(d_file);
					
					removeDeleteFileFromList(d_file);
				}
			}
			
			else if(transaction.operation_name.equals("write")) {
				
				PendingFile w_file=getFileToWrite(transaction.getFile_name());
				
				if(w_file!=null) {
					
					writeFile(w_file,transaction.getRFile());
					
					removeWriteFileFromList(w_file);
				}
			}
		}
	}

	/**
	 * Aborts the Transaction with Transaction ID.
	 */
	@Override
	public void doAbort(long tran_id) throws SystemException, TException {
		// TODO Auto-generated method stub
		
		Transaction transaction=local_participant.getTransactionInfo(tran_id);
		
		if(transaction!=null) {
		
			local_participant.updateTransactionInList(tran_id, false);
			
			local_participant.serializeTransactions();
			
			if(transaction.operation_name.equals("delete")) {
			
				PendingFile d_file=getFileToDelete(transaction.getFile_name());
		
				if(d_file!=null)
				{
					clearPendingFileData(d_file);
					
					removeDeleteFileFromList(d_file);
				}
			}
			
			else if(transaction.operation_name.equals("write")) {
				
				PendingFile w_file=getFileToWrite(transaction.getFile_name());
				
				if(w_file!=null) {
					
					clearPendingFileData(w_file);
					
					removeWriteFileFromList(w_file);
				}
			}
		}
	}


	/**
	 * Returns Object of RFile which includes File Content, File Name and Owner
	 * of File.
	 */
	@Override
	public RFile readFile(Transaction tran_info) throws SystemException, TException {
		// TODO Auto-generated method stub
		
		String filename=tran_info.getFile_name();
		
		RFile rFile =null;
		
		try {
			
			rFile = tryReadLock(filename);
		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		finally {
			
			if(rFile!=null)
				rFile.setClientID(tran_info.getClient_id());
		
			return rFile;
		}
	}
	
	
	/**
	 * This method first tries to acquire lock on file with given file name. If
	 * lock is acquired, it creates the object of RFile and includes it with
	 * File Content, Owner and Name of File. If lock was failed to acquire, null
	 * value is returned.
	 * 
	 * @param filename
	 *            Name of file to read.
	 * @return Object of RFile.
	 * @throws IOException
	 *             If any error occurs while reading a file, this exception is
	 *             thrown.
	 */
	private RFile tryReadLock(String filename) throws IOException {
		
		RFile rFile=null;
		
		boolean ready=true;
		
		RandomAccessFile readFile = null;

		FileChannel readChannel = null;

		FileLock readLock = null;
		
		try {
			
			File lockFile = new File(filename);
			
			if(lockFile.exists()) {
				
					readFile = new RandomAccessFile(lockFile, "rw");

					readChannel = readFile.getChannel();

					readLock = readChannel.tryLock();

					if (readLock != null) {
					
						//lockFile.deleteOnExit();
						
						rFile=getFileFromDisk(filename);
					}
			}

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		finally {
			
			if (readLock != null && readLock.isValid())
		        readLock.release();
		    
			if (readFile != null)
		        readFile.close();
		}
		
		return rFile;
	}
	
	
	/**
	 * This method manages write request from Coordinator. It acquires a lock on
	 * file to write and holds it until it receives final decision from Coordinator.
	 * 
	 * @param file
	 *            Object of RFile.
	 * @return True, if lock is acquired, false otherwise.
	 * @throws IOException
	 *             If any error occurs while accessing file, this exception is
	 *             thrown.
	 */
	public boolean canWriteCommit(RFile file) throws IOException {
		
		boolean writeFlag=false;
		
		RandomAccessFile writeFile=null;
		
		FileChannel writeChannel=null;
		
		FileLock writeLock=null;
		
		try {
		
				File lockFile=new File(file.getFilename());
		
				writeFile=new RandomAccessFile(lockFile, "rw");
		
				writeChannel=writeFile.getChannel();
			
				writeLock=writeChannel.tryLock();
			
				if(writeLock!=null){
						
					//lockFile.deleteOnExit();
					
					PendingFile fileToWrite=new PendingFile(file.getFilename(),lockFile,writeLock,writeChannel,writeFile);
					
					addWriteFileToList(fileToWrite);
					
					writeFlag=true;
				} 
				
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		finally {  
			
		      return writeFlag;
		}
	}
	
	
	private RFile getFileFromDisk(String filename) {
		
		RFile rFile=new RFile();
		
		try {
			
				BufferedReader bufferReader=new BufferedReader(new FileReader(filename));
			
				StringBuilder stringBuilder= new StringBuilder();
	
				String readLine="";
	
				while((readLine=bufferReader.readLine())!=null) {
		
					stringBuilder.append(readLine+"\n");
				}
	
				String fileContent=stringBuilder.toString();
		
				rFile.setContent(fileContent);
		
				rFile.setFilename(filename);
				
		}catch(Exception e) {
		
			System.out.println("Exception is "+e.getMessage());
		}
		
		return rFile;
	}
	
	/**
	 * This method manages delete request from Coordinator. It acquires a lock
	 * on file to delete and holds it until it receives final decision from
	 * Coordinator.
	 * 
	 * @param filename
	 *            File Name to Delete.
	 * @return True if lock is acquired, false otherwise.
	 * @throws IOException
	 *             If any error occurs while accessing file, this exception is
	 *             thrown.
	 */
	@SuppressWarnings("finally")
	public boolean canDeleteCommit(String filename) throws IOException {
		
		boolean deleteFlag=false;
		
		RandomAccessFile deleteFile=null;
		
		FileChannel deleteChannel=null;
		
		FileLock deleteLock=null;
		
		try {
		
				File lockFile=new File(filename);
		
				if(lockFile.exists()) {
					
					deleteFile=new RandomAccessFile(lockFile, "rw");
		
					deleteChannel=deleteFile.getChannel();
			
					deleteLock=deleteChannel.tryLock();
			
					if(deleteLock!=null) {
				
						PendingFile fileToDelete=new PendingFile(filename,lockFile,deleteLock,deleteChannel,deleteFile);
					
						addDeleteFileToList(fileToDelete);
					
						lockFile.deleteOnExit();
					
						deleteFlag=true;
					} 
				}
				
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		finally {
		       
		    return deleteFlag;
		}	
	}
	
	
	/**
	 * Deletes the file from local disk of Participant and releases lock
	 * acquired.
	 * 
	 * @param d_file
	 *            Object of PendingFile to delete.
	 */
	private void deleteFile(PendingFile d_file) {
		
		if(d_file.filelock!=null && d_file.filelock.isValid() && d_file.file!=null) {
			
			try {
				
				d_file.file.delete();
			
				d_file.filelock.release();
				
				d_file.randomFile.close();
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Writes the file to local disk of Participant. After write operation is
	 * done, it releases the lock acquired.
	 * 
	 * @param w_file
	 *            Object of Pending File which includes File Name, File Lock,
	 *            File Channel.
	 * @param rFile
	 *            Object of RFile.
	 */
	private void writeFile(PendingFile w_file,RFile rFile) {
		
		if(w_file.filelock!=null && w_file.filelock.isValid() && w_file.file!=null) {
			
			try {
				
				w_file.fileChannel.write(ByteBuffer.wrap(rFile.getContent().getBytes()));	
			
				w_file.fileChannel.force(false);
				
				w_file.filelock.release();
				
				w_file.randomFile.close();
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	
	public PendingFile getFileToWrite(String filename) {
		
		synchronized (write_lock_object) {
			
			for(PendingFile w_fileElement:filesToWrite) {
				
				if(w_fileElement.filename.equals(filename))
					return w_fileElement;
				
			}
			
			return null;
		}
	}
	
	
	
	public void addWriteFileToList(PendingFile w_file) {
		
		synchronized (write_lock_object) {
			
			if(filesToWrite!=null){
				
				filesToWrite.add(w_file);
				
			}
		}
	}
	
	
	public void removeWriteFileFromList(PendingFile w_file) {
		
		synchronized (write_lock_object) {
			
			for(PendingFile w_fileElement:filesToWrite) {
				
				if(w_fileElement.filename.equals(w_file.filename))
				{
					filesToWrite.remove(w_fileElement);
					
					break;
				}
			}
		}
	}
	
	/**
	 * This method includes functionality to release lock acquired for file and
	 * clears respective entry from ist of Pending File.
	 * 
	 * @param p_file
	 *            Object of Pending File which includes File Name, File Lock,
	 *            File Channel.
	 */
	private void clearPendingFileData(PendingFile p_file) {
		 
		try {
			
			if (p_file.filelock != null && p_file.filelock.isValid())
				p_file.filelock.release();
	     
			//if (p_file.randomFile!= null)
				//p_file.randomFile.close();
			
			System.out.println("Clear pending data");
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void addDeleteFileToList(PendingFile d_file) {
		
		synchronized (delete_lock_object) {
			
			if(filesToDelete!=null){
				
				filesToDelete.add(d_file);
				
			}
		}
	}
	
	public void removeDeleteFileFromList(PendingFile d_file) {
		
		synchronized (delete_lock_object) {
			
			for(PendingFile d_fileElement:filesToDelete) {
				
				if(d_fileElement.filename.equals(d_file.filename)) {
					
					filesToDelete.remove(d_fileElement);
					
					break;
					
				}
			}
		}
	}
	
	
	public PendingFile getFileToDelete(String filename) {
		
		synchronized (delete_lock_object) {
			
			for(PendingFile d_fileElement:filesToDelete) {
				
				if(d_fileElement.filename.equals(filename))
					return d_fileElement;
				
			}
			
			return null;
		}
	}
	
	
	public class PendingFile {
		
		private String filename=null;
		private File file=null;
		private FileLock filelock=null;
		private FileChannel fileChannel=null;
		private RandomAccessFile randomFile=null;
		
		
		public PendingFile(String filename,File file,FileLock filelock,FileChannel fileChannel, RandomAccessFile randomFile) {
			
			this.filename=filename;
			
			this.file=file;
			
			this.filelock=filelock;
			
			this.fileChannel=fileChannel;
			
			this.randomFile=randomFile;
		}	
	}
}
