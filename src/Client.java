import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * The class Client is responsible for creating request with desired file
 * operation and sends it to the Controller.
 * 
 * @author chetan
 *
 */
public class Client {

	public String hostName;
	public int hostPort;
	public String todoOperation;
	public String fileName;
	public String clientID;
	public List<ClientDAO> clientOperationList;
	
	public static void main(String[] args) {
	
		Client client=new Client();
		
		if(client.parseArguments(args)) 
			client.performOperation();
	}
	
	/**
	 * Parses command line arguments with requisite parameters. Each operation is stores in client list.
	 * @param args Command Line Arguments.
	 * @return True if parsing is successful, false otherwise.
	 */
	private boolean parseArguments(String[] args) {
		
		boolean flag = false;

		try {
			
			File operations_file = new File(args[0]);

			Scanner input = new Scanner(operations_file);

			clientOperationList = new ArrayList<ClientDAO>();

			while (input.hasNextLine()) {
				
				String line = input.nextLine();

				line = line.trim();

				String[] details = line.split(" ");
				
				ClientDAO clientElement = new ClientDAO();

				if (!details[0].equals(null) && !details[1].equals(null)) {

					clientElement.hostname = details[0];

					clientElement.hostport = Integer.valueOf(details[1]);
				}

				int i = 2;

				while (i < details.length) {

					if (details[i].equals("--operation"))
						clientElement.operationName = details[++i];

					if (details[i].equals("--filename"))
						clientElement.fileName = details[++i];

					if (details[i].equals("--client"))
						clientElement.clientName = details[++i];

					i++;
				}
				
				clientOperationList.add(clientElement);
			}

			if (input != null)
				input.close();

			flag=true;
			
		} catch (Exception e) {

			
			e.printStackTrace();
		}
		
		return flag;

	}
	
	
	private void performOperation() {
		
		for(int i=0;i<clientOperationList.size();i++) {
			
			sendClientRequest(clientOperationList.get(i));
		}
	}

	
	/**
	 * Sends client request to the Controller.
	 * 
	 * @param clientElement
	 *            Object of ClientOperation which includes details of operation to be
	 *            performed.
	 */
	private void sendClientRequest(ClientDAO clientElement) {
		
		try {

			TTransport clientTransport;

			clientTransport = new TSocket(clientElement.hostname, clientElement.hostport);

			clientTransport.open();

			TProtocol protocol = new TBinaryProtocol(clientTransport);

			FileStore.Client client = new FileStore.Client(protocol);

			if (clientElement.operationName.equals("read")) {
				
				RFile readFile = client.readFile(clientElement.fileName,clientElement.clientName);
				
				if(readFile!=null && readFile.getContent()!=null) {
					System.out.println("File Content is "+readFile.getContent());
				}
				else {
					System.out.println("An error occurred while accessing file. Please make sure your file exists !!");
				}
			}

			else if (clientElement.operationName.equals("write")) {

				RFile localFile = getFileInfo(clientElement);

				if(localFile!=null) {
				
					StatusReport writeStatus = client.writeFile(localFile);

					if(writeStatus.status==Status.SUCCESSFUL)
						System.out.println("Write transaction successful !!");
					else
						System.out.println("Write transaction failed !!");
				}
			}

			else if(clientElement.operationName.equals("delete")) {
				
				StatusReport deleteStatus = client.deleteFile(clientElement.fileName,clientElement.clientName);

				if(deleteStatus.status==Status.SUCCESSFUL)
					System.out.println("Delete transaction successful !!");
				else
					System.out.println("Delete transaction failed !!");
				
			}

			if(clientTransport!=null)
				clientTransport.close();
		
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	
	/**
	 * Reads local file into RFile object
	 * 
	 * @param clientElement
	 *            Object of Client used to fill in details of desired operation.
	 * @return Object of RFile which includes FileName, File Content and Owner
	 *         of the File.
	 */
	private RFile getFileInfo(ClientDAO clientElement) {

		RFile localFile = null;

		try {

			BufferedReader bufferReader = new BufferedReader(new FileReader(clientElement.fileName));

			StringBuilder stringBuilder = new StringBuilder();

			String readLine = "";

			while ((readLine = bufferReader.readLine()) != null) {

				stringBuilder.append(readLine + "\n");
			}

			String fileContent = stringBuilder.toString();

			localFile = new RFile();

			localFile.setFilename(clientElement.fileName);
			
			localFile.setContent(fileContent);
			
			localFile.setClientID(clientElement.clientName);

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			System.out.println("File " + fileName + " not found. Please make sure your file exists !!");

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return localFile;
	}

	
	class ClientDAO {
		
		String hostname;
		int hostport;
		String operationName;
		String fileName;
		String clientName;
		
	}
	
}
