/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());


	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
    if (curMemList.size() > 1 && curMemList != ring) {
        ring = curMemList;
        stabilizationProtocol();
    }
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
    for (i = 0; i < this->memberNode->memberList.size(); i++) {
        Address addressOfThisMember;
        int id = this->memberNode->memberList.at(i).getid();
        short port = this->memberNode->memberList.at(i).getport();
        memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
        memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
        curMemList.emplace_back(Node(addressOfThisMember));
        //add the curr node to the ring list
        
    }
    curMemList.push_back(Node(memberNode->addr));
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
    vector<Node> nodevec = findNodes(key);
    trans_map.insert({ g_transID,transactions(key,value,"create",par->getcurrtime()) });
    for (Node n : nodevec) {
        if (n.nodeAddress == memberNode->addr) {
            ht.insert({ key,value });
            trans_map[g_transID].success++;
            trans_map[g_transID].count++;
            log->logCreateSuccess(&memberNode->addr, false, g_transID, key, value);
        }
        else {
            emulNet->ENsend(&memberNode->addr, &n.nodeAddress, Message(g_transID, memberNode->addr, CREATE, key, value, PRIMARY).toString());
        }
    }
    myq.push(g_transID);
    g_transID++;
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	/*
	 * Implement this
	 */
    vector<Node> nodevec = findNodes(key);
    trans_map.insert({ g_transID,transactions(key,"","read",par->getcurrtime()) });
    for (Node n : nodevec) {
        if (n.nodeAddress == memberNode->addr) {
        	if (ht.count(key)){
        		trans_map[g_transID].value = ht[key];
        		trans_map[g_transID].success++;
        		log->logReadSuccess(&memberNode->addr, false, g_transID, key,trans_map[g_transID].value);
        	}
        	else log->logReadFail(&memberNode->addr, false, g_transID, key);
            trans_map[g_transID].count++;
        }
        else {
            emulNet->ENsend(&memberNode->addr, &n.nodeAddress, Message(g_transID, memberNode->addr, READ, key).toString());
        }
    }
    myq.push(g_transID);
    g_transID++;
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
    vector<Node> nodevec = findNodes(key);
    int count = 0;
    trans_map.insert({ g_transID,transactions(key,value,"create",par->getcurrtime()) });
    for (Node n : nodevec) {
        if (n.nodeAddress == memberNode->addr) {
        	if (ht.count(key)){
        		ht[key] = value;
        		trans_map[g_transID].success++;
        		log->logUpdateSuccess(&memberNode->addr, false, g_transID, key, value);
        	}
   			else log->logUpdateFail(&memberNode->addr, false, g_transID, key, value);
            trans_map[g_transID].count++;
            
        }
        else {
            count++;
            emulNet->ENsend(&memberNode->addr, &n.nodeAddress, Message(g_transID, memberNode->addr, UPDATE, key, value, PRIMARY).toString());
        }
    }
    myq.push(g_transID);
    g_transID++;
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
    vector<Node> nodevec = findNodes(key);
    int count = 0;
    trans_map.insert({ g_transID,transactions(key,"","delete",par->getcurrtime()) });
    for (Node n : nodevec) {
        if (n.nodeAddress == memberNode->addr) {
            if (ht.count(key)){
            	trans_map[g_transID].success++;
            	ht.erase(key);
            	log->logDeleteSuccess(&memberNode->addr, false, g_transID, key);
           	}
            else log->logDeleteFail(&memberNode->addr, false,g_transID, key);
            trans_map[g_transID].count++;
        }
        else {
            count++;
            emulNet->ENsend(&memberNode->addr, &n.nodeAddress, Message(g_transID, memberNode->addr, DELETE,key).toString());
        }
    }
    myq.push(g_transID);
    g_transID++;
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Insert key, value, replicaType into the hash table
    ht.insert({ key,value });
    return true;
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
    /*
     * Implement this
     */
     // Read key from local hash table and return value
    if (ht.count(key)) {
        return ht.at(key);
    }
    else return "";
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Update key in local hash table and return true or false
    if (ht.count(key)) {
        ht[key] = value;
        return true;
    }return false;
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
	/*
	 * Implement this
	 */
	// Delete the key from the local hash table
    if (ht.count(key)) {
        ht.erase(key);
        return true;
    }
    return false;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;

	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);

		/*
		 * Handle the message types here
		 */
        Message currmsg(message);
        bool trans;
        string value;
        switch (currmsg.type) {
        case (CREATE):
            if (ht.count(currmsg.key)) {
                trans = true;
                break;
            }
            trans = createKeyValue(currmsg.key, currmsg.value, currmsg.replica);
            if (trans) log->logCreateSuccess(&memberNode->addr, false, currmsg.transID, currmsg.key, currmsg.value);
            else log->logCreateFail(&memberNode->addr, false, currmsg.transID, currmsg.key, currmsg.value);
            break;
        case(UPDATE):
            trans = updateKeyValue(currmsg.key, currmsg.value, currmsg.replica);
            if (trans) log->logUpdateSuccess(&memberNode->addr, false, currmsg.transID, currmsg.key, currmsg.value);
            else log->logUpdateFail(&memberNode->addr, false, currmsg.transID, currmsg.key, currmsg.value);
            break;
        case (DELETE):
            trans = deletekey(currmsg.key);
            if (trans) log->logDeleteSuccess(&memberNode->addr, false, currmsg.transID, currmsg.key);
            else log->logDeleteFail(&memberNode->addr, false, currmsg.transID, currmsg.key);
            break;
        case(READ): 
            value = readKey(currmsg.key);
            trans = !value.empty();
            if (trans) log->logReadSuccess(&memberNode->addr, false, currmsg.transID, currmsg.key,value);
            else log->logReadFail(&memberNode->addr, false, currmsg.transID, currmsg.key);
            break;
        case(REPLY):
        	if (!trans_map.count(currmsg.transID)) continue;
            trans_map[currmsg.transID].success += currmsg.success;
            trans_map[currmsg.transID].count++;
            if (trans_map[currmsg.transID].success >=2) {
                logtrans(currmsg.transID,true);
                trans_map.erase(currmsg.transID);

            }
            else if (trans_map[currmsg.transID].count == 3) {
                logtrans(currmsg.transID, false);
                trans_map.erase(currmsg.transID);
            }
            continue;
        case (READREPLY):
        	if (!trans_map.count(currmsg.transID)) continue;
            trans = !currmsg.value.empty();
            trans_map[currmsg.transID].success += trans;
            if (trans) trans_map[currmsg.transID].value = currmsg.value;
            trans_map[currmsg.transID].count++;
            if (trans_map[currmsg.transID].success >= 2) {
                logtrans(currmsg.transID, true);
                trans_map.erase(currmsg.transID);

            }
            else if (trans_map[currmsg.transID].count == 3) {
                logtrans(currmsg.transID, false);
                trans_map.erase(currmsg.transID);
            }
            continue;
        }
        if (currmsg.type == READREPLY || currmsg.type == REPLY)cout<<"SHOULD NOT REACH HERE"<<endl;
        //send a reply msg
        if (currmsg.type != READ) emulNet->ENsend(&memberNode->addr, &currmsg.fromAddr, Message(currmsg.transID, memberNode->addr, REPLY, trans).toString());
        else emulNet->ENsend(&memberNode->addr, &currmsg.fromAddr, Message(currmsg.transID, memberNode->addr, value).toString());

	}
	//check the queue for timed out transactions
	while (!myq.empty()){
		int id = myq.front();
		cout<<"transaction id = "<<id<<endl;
		if (trans_map.count(id) && par->getcurrtime() - trans_map.at(id).time >=15) logtrans(myq.front(),false);
		myq.pop();
	}
}

void MP2Node::logtrans(int id, bool success) {
    string type = trans_map[id].type;
    string key = trans_map[id].key;
    string value = trans_map[id].value;
    if (type == "delete"){
        if (success) log->logDeleteSuccess(&memberNode->addr, true, id, key);
        else log->logDeleteFail(&memberNode->addr, true, id,key);
    }
   	else if (type == "update"){
        if (success) log->logUpdateSuccess(&memberNode->addr, true, id, key,value);
        else log->logUpdateFail(&memberNode->addr, true, id, key,value);
    }
    else if (type == "create"){
        if (success) log->logCreateSuccess(&memberNode->addr, true, id, key, value);
        else log->logCreateFail(&memberNode->addr, true, id, key, value);
    }
    else if (type == "read"){
        if (success) log->logReadSuccess(&memberNode->addr, true, id, key, value);
        else log->logReadFail(&memberNode->addr, true, id, key);
    }
    else if (type == "replicate"){
        ReplicateKey(key);
    }
    else if (type == "deletekey"){
        if (success) ht.erase(key);
        else ReplicateKey(key);
    }
    return;
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */

/* MY IMPLEMENTATION
* Each node will check its list of keys that it has and try to replicate them, this is simple at the cost of bandwidth
*/
void MP2Node::stabilizationProtocol() {
    for (auto it = ht.begin(); it != ht.end(); it++) {
        ReplicateKey(it->first);
    }
}
void MP2Node::ReplicateKey(string key) {
    vector<Node> nodevec = findNodes(key);
    int count = 0;
    bool in = false;
    for (Node n : nodevec) {
        if (n.nodeAddress == memberNode->addr) in = true;
        else {
            emulNet->ENsend(&memberNode->addr, &n.nodeAddress,Message(g_transID, memberNode->addr, CREATE, key, ht[key], PRIMARY).toString());
            count++;
        }
    }
    string type;
    if (!in) type = "deletekey";
    else type = "replicate";
    trans_map.insert({ g_transID,transactions(key,"",type,par->getcurrtime()) });
    if (in){
    	trans_map[g_transID].success++;
    	trans_map[g_transID].count++;
    }
    myq.push(g_transID);
    g_transID++;
    
}
