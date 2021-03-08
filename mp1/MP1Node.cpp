/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	//int id = *(int*)(&memberNode->addr.addr);
	//int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(memberNode->addr.addr) + sizeof(long);
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        cout<<"Request join with "<<memberNode->addr.getAddress()<<endl;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1)+sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);
        //cout<<"Message sent"<<endl;
        free(msg);
    }

    return 1;

}
/** 
my implementation
*/
int MP1Node::gettime() {
    return par->getcurrtime();
}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
    return -1;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    
    checkMessages();
    memberNode->heartbeat++;
    // Wait until you're in the group...
    if(!memberNode->inGroup ) {
    	return;
    }
    //cout<<"node "<<memberNode->addr.getAddress()<<" sending HB at time = "<<gettime()<<" with ML size of "<<memberNode->memberList.size()<<endl;
    //cout<<"node "<<memberNode->addr.getAddress()<<" entering node ops"<<endl;
    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
void MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */
    MsgTypes msgtype;
    memcpy(&msgtype,data,sizeof(MsgTypes));
    if (msgtype == JOINREQ) joinreq(data);
    else if (msgtype == HEARTBEAT) {
        memberNode->inGroup = true;
        updatetables(data,size);
    }
    return;
}
/**
* My implementation:
This functions receives a heartbeat from another node and updates its own membership tables
*/
void MP1Node::updatetables(char* data, int size) {
	//cout<<"updating tables"<<endl;
    size_t currsize = sizeof(MessageHdr);
    while (currsize < size_t(size)) {
        long heartbeat;
        memcpy(&heartbeat,data + currsize,sizeof(long));
        char addr [6];
        memcpy(&addr,data +currsize+sizeof(long),sizeof(addr));
        int id;
        short port;
        memcpy(&id, &addr[0], sizeof(int));
		memcpy(&port, &addr[4], sizeof(short));
		string key = to_string(id) + ":" + to_string(port);
		//cout<<key<<endl;
        if (Address(key) == memberNode->addr){
        	;
        }
        //if new node, add it to the table
        else if (!nodetable.count(key)) {
            nodetable.insert({ key,memberNode->memberList.size() });
            Address tempaddr = Address(key);
            log->logNodeAdd(&memberNode->addr,&tempaddr);
            memberNode->memberList.push_back(MemberListEntry(id, port, heartbeat,gettime()));
        }
        else if (heartbeat > memberNode->memberList[nodetable[key]].heartbeat) {
            memberNode->memberList[nodetable[key]].heartbeat = heartbeat;
            memberNode->memberList[nodetable[key]].timestamp = gettime();
        }
        currsize += sizeof(Address) + sizeof(long);
    }
    //cout<<"exiting update tables"<<endl;
    return;
}
/**
My implementation: 
The response when the message is a JOINREQ
1. add the request node into the current nodes membership list.
2. send response back to the request node with all the other nodes in the group
*/
void MP1Node::joinreq(char* data) {
	//cout<<"responding to join req"<<endl;
    char addr [6];
    memcpy(&addr[0],data+sizeof(MessageHdr), sizeof(addr));
    //get id, port
    int id;
    short port;
    memcpy(&id, &addr[0], sizeof(int));
	memcpy(&port, &addr[4], sizeof(short));
	string key = to_string(id) + ":" + to_string(port);
	//cout<<"node "<<memberNode->addr.getAddress()<<" sending ML to "<<Address(key).getAddress()<<endl;
    //get heartbeat
    long heartbeat;
    memcpy(&heartbeat,data+sizeof(MessageHdr)+sizeof(addr), sizeof(long));
    //insert new node into memberlist
    Address tempaddr = Address(key);
    log->logNodeAdd(&memberNode->addr,&tempaddr);
    nodetable.insert({key,memberNode->memberList.size()});
    memberNode->memberList.push_back(MemberListEntry(id, port,heartbeat,gettime()));
    sendML(&tempaddr);
    return;
}

/* 
My implementation:
This function sends the membership list of the current node to another node
as a heartbeat message type
*/
void MP1Node::sendML(Address * addr) {
    MessageHdr* msg;
    size_t msgsize = sizeof(MessageHdr) + (memberNode->memberList.size() +1)* (sizeof(memberNode->addr.addr) + sizeof(long));
    msg = (MessageHdr*) malloc(msgsize * sizeof(char));
    msg->msgType = HEARTBEAT;
    memcpy(msg +1, &memberNode->heartbeat, sizeof(long));
    memcpy((char*)(msg + 1) +sizeof(long), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
    size_t offset = sizeof(MessageHdr) + sizeof(memberNode->addr.addr) + sizeof(long);
    for (unsigned int i = 0; i < memberNode->memberList.size();i++) {
    	char addr [6];
    	if (gettime() - memberNode->memberList[i].timestamp > TFAIL) {
    		msgsize -= sizeof(addr) + sizeof(long);
    		continue;
    	}
        memcpy((char*)(msg)+ offset, &memberNode->memberList[i].heartbeat, sizeof(long));
        int id = memberNode->memberList[i].id;
        short port = memberNode->memberList[i].port;
        memcpy(&addr[0], &id, sizeof(int));
		memcpy(&addr[4], &port, sizeof(short));
        memcpy((char*)msg + (offset+sizeof(long)), &addr, sizeof(addr));
        offset += sizeof(long) +sizeof(addr);
    }
    //cout<<"memberlist size = "<<memberNode->memberList.size()<<", messagesize = "<<msgsize<<endl;
    emulNet->ENsend(&memberNode->addr, addr, (char *)msg, msgsize);
    //cout<<"send completed"<<endl;
    free(msg); 
    return;
}
/**
 * My implementation
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
    for (int i = 0; i < memberNode->memberList.size(); i++) {
    	//cout<<"time since last = "<<gettime() - memberNode->memberList[i].timestamp<<endl;
        if (gettime() - memberNode->memberList[i].timestamp > TREMOVE) {
        	int id = memberNode->memberList[i].id;
        	short port = memberNode->memberList[i].port;
        	Address tempaddr(to_string(id) +":" +to_string(port));
        	log->logNodeRemove(&memberNode->addr,&tempaddr);
        	nodetable.erase(tempaddr.getAddress());
        	memberNode->memberList[i] = memberNode->memberList.back();
        	memberNode->memberList.pop_back();
        	id = memberNode->memberList[i].id;
        	port = memberNode->memberList[i].port;
        	tempaddr = Address(to_string(id) +":" +to_string(port));
        	nodetable[tempaddr.getAddress()] = i;
        	i--;
        }
    }
    //pick two random nodes and send a heartbeat
    int node1;
    Address addr1;
    if (memberNode->memberList.size() > 0){
    	node1 = rand() % memberNode->memberList.size();
    	addr1 = createaddress(memberNode->memberList[node1].id, memberNode->memberList[node1].port);
    	sendML(&addr1);
    }
    if (memberNode->memberList.size() > 1){
    	int node2 = ((rand() % (memberNode->memberList.size()- 1)) + 1 + node1) % memberNode->memberList.size();
    	//cout<<node2<<endl;
    	Address addr2 = createaddress(memberNode->memberList[node2].id, memberNode->memberList[node2].port);
    	sendML(&addr2);
    	//cout<<"curr node: "<<memberNode->addr.getAddress()<<", Sent HB to "<<addr1.getAddress()<<", and "<<addr2.getAddress()<<endl;
    }
    return;
}
Address MP1Node::createaddress(int id, short port) {
    return Address(to_string(id) + ":" + to_string(port));
}
/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
