struct Participant {
  1:string name;
  2:string ip;
  3:i32 port;
}

struct Transaction {
  1:i64 tran_id;
  2:string operation_name;
  3:string client_id;
  4:string file_name;
  5:RFile rFile;
  6:T_Status tran_status;
}

typedef string UserID

exception SystemException {
  1: optional string message
}

enum T_Status {
  PENDING=0;
  COMMIT=1;
  ABORT=2;
}

enum Status {
  FAILED = 0;
  SUCCESSFUL = 1;
}

struct StatusReport {
  1: required Status status;
}

struct RFile {
  1: optional string filename;
  2: optional string content;
  3: optional string clientID;
}

service FileStore {
      
  StatusReport writeFile(1: RFile rFile)
    throws (1: SystemException systemException),
  
  StatusReport deleteFile(1: string filename,2: string clientID)
    throws (1: SystemException systemException),

  RFile readFile(1: string filename,2: string clientID)
    throws (1: SystemException systemException),
  
  void getTransactionStatus(1: i64 tran_id, 2: string participant_ip, 3: i32 participant_port)
    throws (1: SystemException systemException),
}

service Participant_Interface {

  StatusReport canCommit(1: Transaction tran_info)
    throws (1: SystemException systemException),
  
  void doCommit(1: i64 tran_id)
     throws (1: SystemException systemException),

  void doAbort(1: i64 tran_id)
    throws (1: SystemException systemException),

  RFile readFile(1: Transaction tran_info)
    throws (1: SystemException systemException),
}



