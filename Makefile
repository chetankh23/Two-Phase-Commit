LIB_PATH=/home/yaoliu/src_code/local/libthrift-1.0.0.jar:/home/yaoliu/src_code/local/slf4j-log4j12-1.5.8.jar:/home/yaoliu/src_code/local/slf4j-api-1.5.8.jar:/home/yaoliu/src_code/local/log4j-1.2.14.jar

all: clean
	mkdir bin
	mkdir bin/client_classes
	mkdir bin/coordinator_classes
	mkdir bin/participant_classes
	javac -classpath $(LIB_PATH) -d bin/participant_classes/ src/Participant_Handler.java src/Local_Participant.java src/Constants.java gen-java/*
	javac -classpath $(LIB_PATH) -d bin/coordinator_classes/ src/Coordinator_Handler.java src/Coordinator.java src/Constants.java gen-java/*
	javac -classpath $(LIB_PATH) -d bin/client_classes/ src/Client.java src/Constants.java gen-java/*


clean:
	rm -rf bin/


