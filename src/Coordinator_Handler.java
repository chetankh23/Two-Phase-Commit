import java.util.List;

import org.apache.thrift.TException;

/**
 * The class Coordinator_Handler is responsible for handling incoming RPC
 * requests. The type of call determines the required action to be taken.
 * 
 * @author chetan
 *
 */
public class Coordinator_Handler implements FileStore.Iface{
	
	private List<Participant> participant_list;
	private Coordinator coordinator;
	
	
	
	public Coordinator_Handler(Coordinator coordinator,List<Participant> participant_list) {
	
		this.coordinator=coordinator;
		
		this.participant_list=participant_list;
	}


	/**
	 * Handles incoming RPC write file request from client.
	 */
	@Override
	public StatusReport writeFile(RFile rFile) throws SystemException, TException {
		// TODO Auto-generated method stub
		
		StatusReport status_report=new StatusReport();
		
		if(coordinator.manageWrite(rFile))
			status_report.status=Status.SUCCESSFUL;
		else
			status_report.status=Status.FAILED;
		
		return status_report;
	}

	/**
	 * Handles incoming RPC delete file request from client.
	 */
	@Override
	public StatusReport deleteFile(String filename, String clientID) throws SystemException, TException {
		// TODO Auto-generated method stub
		
		StatusReport status_report=new StatusReport();
		
		if(coordinator.manageDelete(filename, clientID))
			status_report.status=Status.SUCCESSFUL;
		
		else
			status_report.status=Status.FAILED;
		
		return status_report;
	}


	/**
	 * Handles incoming RPC read file request from client.
	 */
	@Override
	public RFile readFile(String filename, String clientID) throws SystemException, TException {
		// TODO Auto-generated method stub
		
		RFile rFile=coordinator.manageRead(filename,clientID);
		
		return rFile;
	}
	
	/**
	 * The method getTransactionStatus is called by any of the Participant which
	 * comes back after failure.
	 */
	@Override
	public void getTransactionStatus(long tran_id, String participant_ip, int participant_port)
			throws SystemException, TException {
		// TODO Auto-generated method stub
		
		coordinator.manageParticipantRequest(tran_id,participant_ip,participant_port);
	}	
}
