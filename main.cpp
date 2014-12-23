//Jakub Sygnowski
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <time.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <vector>
#include <mutex>
#include <set>
#include <thread>
using namespace std;
#define MAX_NEIGHBOURS 5
#define MUTEX_NUMBER 1024
#define printf(...) ;
struct logEntry {
    char type, sender;
    int msgId;
};
vector<logEntry> log;
mutex logLock;
volatile bool finished = false;
long listenSocket;
thread writer;
thread relyer[MAX_NEIGHBOURS];
FILE*logFile;
inline void deliver(int sender, int id){
    logLock.lock();
    printf("deliver %d from %d\n", id, sender);
    logEntry entry;
    entry.type = 'd';
    entry.sender = sender;
    entry.msgId = id;
    log.push_back(entry);
    logLock.unlock();
}
inline void broadcastLog(int id){
    logLock.lock();
    printf("broadcast %d request\n", id);
    logEntry entry;
    entry.type = 'b';
    entry.msgId = id;
    log.push_back(entry);
    logLock.unlock();
}

struct sockaddr_in neighAddr[MAX_NEIGHBOURS];
int neighbourNum = 0;
int processNum;
int getNeighbourNumber(struct sockaddr_in address){
    for(int i = 0; i < neighbourNum; i++){
        if (neighAddr[i].sin_port == address.sin_port &&
                neighAddr[i].sin_addr.s_addr == address.sin_addr.s_addr)
            return i;
    }
    return -1;
}
int myNumber, msgNumber;

void writeToNeighbour(int neighbour, int msg, int sender = myNumber);
void writeToEveryNeighbour(int msg, int sender = myNumber);

static int wait_for_start = 1;

static void start(int signum) {
    fprintf(stderr,"starting\n");
	wait_for_start = 0;
}

static void stop(int signum) {
	//reset signal handlers to default
	signal(SIGTERM, SIG_DFL);
	signal(SIGINT, SIG_DFL);
    finished = true;

	//immediately stop network packet processing
	printf("Immediately stopping network packet processing.\n");
	
	//write/flush output file if necessary
	fprintf(stderr,"Writing output.\n");
    logLock.lock();
    for(int i = 0; i < int(log.size()); i++){
        if (log[i].type == 'b'){
            fprintf(logFile,"b %d\n", log[i].msgId);
        } else {
            fprintf(logFile,"d %d %d\n", log[i].sender + 1, log[i].msgId);
        }
    }
    logLock.unlock();
    fclose(logFile);
    close(listenSocket);

    fprintf(stderr,"Exiting from signal handler\n");
    /*
     * Please note that even though I am waiting for those other threads to finish,
     * I already wrote my log. If something gets stuck here, you are free to kill the program - it already did its job.
     */
    for(int i = 0; i < neighbourNum; i++){
        relyer[i].join();
        fprintf(stderr,"%d relyer joined\n", i);
    }
    writer.join();
    fprintf(stderr, "really exiting\n");
    //terminate();
	exit(0);
}

void configureListenSocket(long port, char* address){
    if ((listenSocket = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
        fprintf(stderr, "Cannot open socket\n");
        exit(1);
    }
    struct sockaddr_in myaddr;
    memset((char*) &myaddr, 0, sizeof(myaddr));
    myaddr.sin_family = AF_INET;
    myaddr.sin_port = htons(port);
    myaddr.sin_addr.s_addr = inet_addr(address);
    if (bind(listenSocket, (struct sockaddr *)&myaddr, sizeof(myaddr)) < 0){
        fprintf(stderr, "Cannot bind a socket\n");
        exit(1);
    }
    printf("set up listen socket on %s:%ld\n", address, port);
}

void configureWriteSocket(long port, char* address){
    memset((char*) &neighAddr[neighbourNum], 0, sizeof(neighAddr[neighbourNum]));
    neighAddr[neighbourNum].sin_family = AF_INET;
    neighAddr[neighbourNum].sin_port = htons(port);
    neighAddr[neighbourNum].sin_addr.s_addr = inet_addr(address);
    neighbourNum++;
}

void parseArguments(int argc, char** argv){
    if (argc < 4){
        printf("Too little arguments\n");
        exit(1);
    }
    myNumber = argv[1][0] - '1';
    msgNumber = atoi(argv[argc-1]);
    printf("msgNumber = %d\n", msgNumber);
    argc--;
    char *someAddress;
    long port;
    for(int i = 2; i < argc; i++){
        if (i % 2){
            port = atoi(argv[i]);
            if (((i/2) - 1) == myNumber){
                configureListenSocket(port, someAddress);
            } else {
                configureWriteSocket(port, someAddress);
            }
        } else {
            someAddress = argv[i];
        }
    }
    processNum = neighbourNum + 1;
    printf("neighbourNum %d\n", neighbourNum);
}

vector<char> senders[MAX_NEIGHBOURS];
mutex messageLock[MAX_NEIGHBOURS][MUTEX_NUMBER];
mutex resizing[MAX_NEIGHBOURS];
set<pair<char, int> > nonConfirmed[MAX_NEIGHBOURS];
mutex nonCMutex[MAX_NEIGHBOURS];

/*
 * before writing is started with SIG_USR1, nonConfirmed sets are empty.
 */
int sleepTime[MAX_NEIGHBOURS];
mutex sleepLock[MAX_NEIGHBOURS];
const int tenNine = 1000000000;
const int sleepDefault = 100;
void relayMessages(int neighbour){
    while(!finished){
    struct timespec sleep_time;
/*    sleepLock[neighbour].lock();
	sleep_time.tv_sec = sleepTime[neighbour]/tenNine;
	sleep_time.tv_nsec = sleepTime[neighbour]%tenNine;
    if (sleepTime[neighbour] < tenNine)
        sleepTime[neighbour] *= 2;
    sleepLock[neighbour].unlock();*/
    sleep_time.tv_sec = 0;
    sleep_time.tv_nsec = 100;
	nanosleep(&sleep_time, NULL);
    printf("Relaying messages to neighbour: %d\n", neighbour);
    nonCMutex[neighbour].lock();
    for(auto msg: nonConfirmed[neighbour]){
        writeToNeighbour(neighbour, msg.second, msg.first);
    }
    nonCMutex[neighbour].unlock();
    }
}

void processMsg(int sender, int id, int getFrom, bool ack = false){
/*    if (!(id % 10)){ //do this check every 10 messages only
    sleepLock[getFrom].lock();
    sleepTime[getFrom] = sleepDefault;
    sleepLock[getFrom].unlock();
    }*/
    messageLock[sender][id % MUTEX_NUMBER].lock();
    printf("processing %d from %d\n", id, sender);
    if (int(senders[sender].size()) <= id){
        resizing[sender].lock();
        if (int(senders[sender].size()) <= id){
            printf("resing\n");
            senders[sender].resize(id + 1000);
        }
        resizing[sender].unlock();
    }
    if (senders[sender][id] == 0){
        printf("see this message for the first time\n");
        if (sender == myNumber){
            for(int i = 0; i < neighbourNum; i++){
                nonCMutex[i].lock();
                nonConfirmed[i].insert(make_pair(sender, id));
                nonCMutex[i].unlock();
            }
            senders[sender][id]++;
        } else {
            senders[sender][id] = 2;
            for(int i = 0; i < neighbourNum; i++){
                if (i != getFrom){
                    nonCMutex[i].lock();
                    nonConfirmed[i].insert(make_pair(sender, id));
                    nonCMutex[i].unlock();
                }
            }
        }
    } else {
        nonCMutex[getFrom].lock();
        if (ack && nonConfirmed[getFrom].find(make_pair(sender, id)) != nonConfirmed[getFrom].end()){
            nonConfirmed[getFrom].erase(nonConfirmed[getFrom].find(make_pair(sender,id))); //TODO: thread safety
            senders[sender][id]++;
        } else {
            printf("have seen this message before\n");
            nonCMutex[getFrom].unlock();
            messageLock[sender][id % MUTEX_NUMBER].unlock();
            return;
        }
        nonCMutex[getFrom].unlock();
    }
    if (senders[sender][id] * 2 > processNum && ((senders[sender][id] - 1) * 2) <= processNum){
        printf("reached majority for msg %d from %d\n", id, sender);
        deliver(sender, id);
    }
    messageLock[sender][id % MUTEX_NUMBER].unlock();
}

void reserveMemory(){
    const int initSize = 1024 * 1024; 
    for(int i = 0; i < processNum; i++){
        senders[i].resize(initSize);
    }
}

int currentMsg;
void sendNextMessage(){
    currentMsg++;
    processMsg(myNumber, currentMsg, myNumber);
    broadcastLog(currentMsg);
}

void acknowledge(int neighbour, int msg, int sender);
void waitForMessage(){
    char msg[20];
    memset((char*)&msg, 0, sizeof(msg));
    struct sockaddr_in addr;
    socklen_t addrLen = sizeof(addr);
    int bytes = recvfrom(listenSocket, msg, 20 * sizeof(char), 0, (struct sockaddr*) &addr, &addrLen);

    int neigh = getNeighbourNumber(addr);
    printf("read %d bytes from %d: %s\n", bytes, neigh, msg);
    if (bytes < 3){
        printf("msg too short\n");
    }
    int sender = msg[0] - '0';
    int msgId = atoi(msg+2);
    if (msg[1] == 'A'){
        printf("got ack of %d %d from %d\n", sender, msgId, neigh);
        processMsg(sender, msgId, neigh, true);
    } else {
        printf("%d %d message from %d\n", sender, msgId, neigh);
        acknowledge(neigh, msgId, sender);
        processMsg(sender, msgId, neigh);
    }
}

void passMessage(int neighbour, char* msg){
    int toSend = strlen(msg);
    int bytes = sendto(listenSocket, msg, toSend, 0, (struct sockaddr *)
            &neighAddr[neighbour], sizeof(neighAddr[neighbour]));
    if (bytes != toSend){
        printf("error while sending\n");
    }
}

void writeToNeighbour(int neighbour, int msg, int sender){
 //   char humanAddr[100];
//    in_addr_t receiver = neighAddr[neighbour].sin_addr.s_addr;
//    inet_ntop(AF_INET, &receiver, humanAddr, 100 * sizeof(char));
//    printf("writing %d %d to neighbour at %s:%d\n", sender, msg, humanAddr, 
//            ntohs(neighAddr[neighbour].sin_port));
    char myMsg[20]; //this limits total number of sent messages to 10^(20-2)-1 :P
    memset((char*) &myMsg, 0, sizeof(myMsg)); //TODO: change to 16?
    myMsg[0] = sender + '0';
    myMsg[1] = ' ';
    sprintf(myMsg + 2, "%d", msg);
    passMessage(neighbour, myMsg);
    /*
    int toSend = strlen(myMsg);
    int bytes = sendto(listenSocket, myMsg, toSend, 0, (struct sockaddr *) &neighAddr[neighbour], 
            sizeof(neighAddr[neighbour]));
    printf("sent %d bytes\n", bytes);
    if (bytes != toSend){
        printf("error while sending\n");
    }*/
}

void acknowledge(int neighbour, int msg, int sender){
    printf("acknoledgementing %d %d to neighbour %d\n", sender, msg, neighbour);
    char myMsg[20];
    memset((char*) &myMsg, 0, sizeof(myMsg));
    myMsg[0] = sender + '0';
    myMsg[1] = 'A';
    sprintf(myMsg + 2, "%d", msg);
    passMessage(neighbour, myMsg);
}

void writeToEveryNeighbour(int msg, int sender){
    for(int i = 0; i < neighbourNum; i++){
        writeToNeighbour(i, msg, sender);
    }
}

void broadcastMessages(int number){
	struct timespec sleep_time;
    while(wait_for_start) {
		sleep_time.tv_sec = 0;
		sleep_time.tv_nsec = 1000;
		nanosleep(&sleep_time, NULL);
	}
    printf("starting to broadcast\n");
    while(!finished && (number < 0 || number--)){
        sleep_time.tv_nsec = 100; // dont use all bandwidth on writing
        nanosleep(&sleep_time, NULL);
/*        logLock.lock();
        if (log.size() > 1000){
            for(int i = 0; i < int(log.size()); i++){
                if (log[i].type == 'b'){
                    fprintf(logFile,"b %d\n", log[i].msgId);
                } else {
                    fprintf(logFile,"d %d %d\n", log[i].sender + 1, log[i].msgId);
                }
            }
            log.clear();
            fprintf(stderr, "flushed log vector\n");
        }
        logLock.unlock();*/
        sendNextMessage();
    }
}

void readMessages(){
    while(!finished){
        printf("waiting for message\n");
        waitForMessage();
        printf("read first message\n");
    }
}

void simulate(){
    while(1){
        char c;
        scanf(" %c", &c);
        if (c == 'R'){
            while(1)
                waitForMessage();
        } else if (c == 'W'){
            thread (broadcastMessages,msgNumber).detach();
        }
    }
}

void dummy(){
    while(1){
        struct timespec sleep_time;
		sleep_time.tv_sec = 1;
		sleep_time.tv_nsec = 1000;
		nanosleep(&sleep_time, NULL);
        wait_for_start = 0;

        printf("Im sleepy\n");
    }
}

void setupThreads(){
    for(int i = 0; i < neighbourNum; i++){
        sleepTime[i] = 1000; //1 ms of wait
        relyer[i] = thread (relayMessages, i);
    }
    printf("relying threads set up\n");
    writer = thread(broadcastMessages,msgNumber);
}

int main(int argc, char** argv) {
	//set signal handlers
	signal(SIGUSR1, start);
	signal(SIGTERM, stop);
	signal(SIGINT, stop);

	parseArguments(argc, argv);
    reserveMemory();
    char filename[] = "da_proc_n.out";
    filename[8] = myNumber + '1';
    logFile = fopen(filename, "w");


	//initialize application
	//start listening for incoming UDP packets
	printf("Initializing.\n");
    setupThreads();

    readMessages();
    //simulate();

	//wait until stopped
	/*while(1) {
		struct timespec sleep_time;
		sleep_time.tv_sec = 1;
		sleep_time.tv_nsec = 0;
		nanosleep(&sleep_time, NULL);
	}*/
    printf("Finishing\n");
}
