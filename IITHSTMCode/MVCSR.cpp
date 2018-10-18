/* command to compile class
g++ -std=gnu++0x MVCSR.cpp -I/home/anila/Desktop/mini/tbb42_20130725oss/include  -Wl,-rpath,/home/anila/Desktop/mini/tbb42_20130725oss/build/linux_intel64_gcc_cc4.6.1_libc2.13_kernel3.0.0_release -L/home/anila/Desktop/mini/tbb42_20130725oss/build/linux_intel64_gcc_cc4.6.1_libc2.13_kernel3.0.0_release  -ltbb
*/

#include "MVCSR.h"
#include<iostream> 
typedef multimap<int,int>::iterator mapit;
/*constructors and destructors for graph and MVCSR classes. No*/
Graph::Graph()     
{
  vertex* temp=new vertex;
  temp->TID=0;
  temp->status=COMMITTED;                        /*transaction just started. so, status= active*/
  vertices.insert(std::pair<int,vertex*>(0,temp));
}
Graph::~Graph()                               /* Graph destructor frees memory alloted to vertices present in hash map*/
{
  map<int,vertex*>::iterator itr;
  itr=vertices.begin();
  for(;itr!=vertices.end();itr++)
  {
    delete(itr->second);
  }
}
MVCSR::MVCSR(const MVCSR& orig) 
{
} 
MVCSR::~MVCSR()                                   /*MVCSR destructor frees dynamic memory allocated to graph*/
{
  delete(graph);
}

/*add_vertex method of graph class takes TID as input and inserts a new vertex into the graph corresponding to new transaction*/

void Graph::add_vertex(int TID)
{
  vertex* temp=new vertex;
  temp->TID=TID;
  temp->status=ACTIVE;                        /*transaction just started. so, status= active*/
  vertices.insert(std::pair<int,vertex*>(TID,temp));
  map<int,vertex*>::iterator itr;
  itr=vertices.begin();
  for(;itr!=vertices.end();itr++)             /* iterating over vertices hash map to add real time edges */
  {
    if(itr->second->status==COMMITTED)        /*only committed transactions should be considered for adding real time edges*/
    {
      add_edge(itr->second->TID,TID);
    }
  }
   
}
/*adds edge in the graph from vertex with TID=TID1 to vertex with TID=TID2 if the edge is not already present*/
void Graph::add_edge(int TID1,int TID2)
{
  if(TID1==TID2)                              /*self loop is not allowed since a transaction does not conflict with itself*/
    return;
  vertex* temp=vertices[TID1];
  list<int>::iterator itr;
  itr=(temp->out_edges).begin();
  for(;itr!=(temp->out_edges).end();itr++)   /*Iterate over the out going edges from TID1 and check if edge already exists to TID2*/
  {
    if(*itr==TID2)
    {  
      return;
    }
  }
  /*if edge does not exist, add it*/
    temp->out_edges.push_back(TID2);        
    temp=vertices[TID2];
    temp->in_edges.push_back(TID1);
}


/*Takes TID as input and deletes the vertex from graph and all incoming and outgoing edges*/
void Graph::delete_vertex(int TID)
{
  /* Deleting incoming edges of current vertex
    Iterate over vertices in in_edges list and delete current vertex from each of their out_edges list*/
  list<int>::iterator in_edges_itr=vertices[TID]->in_edges.begin();
  for(;in_edges_itr!=vertices[TID]->in_edges.end();in_edges_itr++) 
  {
    vertex* source=vertices[*in_edges_itr];
    source->out_edges.remove(TID);
  }
  /* Deleting outgoing edges of current vertex
     Iterate over vertices in out_edges list and delete current vertex from each of their in_edges list*/
  list<int>::iterator out_edges_itr=vertices[TID]->out_edges.begin();
  for(;out_edges_itr!=vertices[TID]->out_edges.end();out_edges_itr++) 
  {
    vertex* destination=vertices[*out_edges_itr];
    destination->in_edges.remove(TID);
  }
  /*deleting temporary in_edges*/
   mapit itr=vertices[TID]->local_out_edges.begin();
   while(itr!=vertices[TID]->local_out_edges.end())
   {
     ((vertices[itr->second])->local_in_edges).erase(TID);
     itr++;
   }
   /*deleting temporary out_edges*/
   itr=vertices[TID]->local_in_edges.begin();
   while(itr!=vertices[TID]->local_in_edges.end())
   {
     ((vertices[itr->second])->local_out_edges).erase(TID);
     itr++;
   }
  /*Delete vertex from hash map*/
  vertices.erase(TID);
}

/*checkfor cycle in graph using DFS algorithm
  Returns true if a cycle is detected and false otherwise*/

bool Graph::check_cycles(int TID,set<int> mark_set,set<int> &visited,int current_TID)
{
  if(mark_set.count(TID))
    return true;
  mark_set.insert(TID);
  list<int>::iterator itr = vertices[TID]->out_edges.begin();
  for(;itr!=vertices[TID]->out_edges.end();itr++)
  {
     /*ignore active transactions except the current transaction that is being checked for correctness*/
     if(visited.count(*itr)||vertices[*itr]== NULL||vertices[*itr]->status== ACTIVE)   
     continue;
     bool res=check_cycles(*itr,mark_set,visited,current_TID);
     if(res==true)
     {
       return true;
     }
  }
  //check local outedges due to current transaction
  mapit m_itr;
  pair<mapit,mapit> active_edges= (vertices[TID]->local_out_edges).equal_range(current_TID);
  for (m_itr = active_edges.first;  m_itr != active_edges.second;  ++m_itr)
  {
     if(visited.count((*m_itr).second)||vertices[m_itr->second]==NULL||vertices[m_itr->second]->status== ACTIVE)   
     continue;
     bool res=check_cycles((*m_itr).second,mark_set,visited,current_TID);
     if(res==true)
     {
       return true;
     }
  }

  visited.insert(TID);
  return false;
}
/*takes TID as input and makes all its edges permanent by moving them from temporary hash map to adjacency list*/
void Graph::make_edges_permanent(int TID)
{
   mapit itr=vertices[TID]->local_out_edges.begin();
   /*copying edges from temporary hash maps to permanent adjacency lists*/
   while(itr!=vertices[TID]->local_out_edges.end())
   {
     add_edge(TID,itr->second);
     itr++;
   }
   itr=vertices[TID]->local_in_edges.begin();
   while(itr!=vertices[TID]->local_in_edges.end())
   {
     add_edge(itr->second,TID);
     itr++;
   }
   /*deleting temporary edges*/
   itr=vertices[TID]->local_out_edges.begin();
   while(itr!=vertices[TID]->local_out_edges.end())
   {
     ((vertices[itr->second])->local_in_edges).erase(TID);
     itr++;
   }
   itr=vertices[TID]->local_in_edges.begin();
   while(itr!=vertices[TID]->local_in_edges.end())
   {
     ((vertices[itr->second])->local_out_edges).erase(TID);
     itr++;
   }
}

/*function to add temporary edge in the hash map*/
void Graph::add_local_edge(int TID,int from_TID,int to_TID)
{
    (vertices[from_TID])->local_out_edges.insert(pair<int,int>(TID,to_TID));
    (vertices[to_TID])->local_in_edges.insert(pair<int,int>(TID,from_TID));
}

/*
add one transaction to effected transactions set of another transaction(TID)
*/
void Graph::insert_effected(int TID,int to_add)
{
    vertices[TID]->effected_transactions.insert(to_add);
}

/*function to delete elements from effected set of transaction*/
void Graph::delete_effected(int TID,int to_del)
{
  pair<multiset<int>::iterator,multiset<int>::iterator> set_itr=vertices[TID]->effected_transactions.equal_range(to_del);
  //multiset<int>::iterator set_itr=vertices[TID]->effected_transactions.find(to_del);
  vertices[TID]->effected_transactions.erase(set_itr.first);                             /*to be verified************************/
}

/*function to remove a transaction from read lists of data items when it is aborting*/
void Graph::remove_from_readlists(STM* lib,int TID)
{
  list<long long>::iterator itr=vertices[TID]->read_set.begin();
  hash_STM::accessor a_tOB;
  MVCSR_tOB* tob_temp;
  for(;itr!=vertices[TID]->read_set.begin();itr++)  // traversing all the data elements read
  {
      lib->sh_tOB_list.find(a_tOB,*itr);
	    tob_temp=(MVCSR_tOB*)(a_tOB->second);
	    list<pair<int,MVCSR_version_data*>>::iterator l_itr; 
	    l_itr=tob_temp->MVCSR_version_list.begin();
	    for(;l_itr!=tob_temp->MVCSR_version_list.end();l_itr++)
	    {
	      if(l_itr->first==TID)
	        break;
	    }
	    tob_temp->MVCSR_version_list.erase(l_itr);   //erasing TID from read set of data element version read
      a_tOB.release();
  }
}

/*function to delete edges from temporary hash map*/
void Graph::delete_temp_edge(int TID,int from_TID,int to_TID)
{
  pair<mapit,mapit> range=(vertices[from_TID]->local_out_edges).equal_range(TID);
  mapit it = range.first;
  while (it != range.second)
  {
    if (it->second == to_TID)
      (vertices[from_TID]->local_out_edges).erase(it++);
    else
      ++it;
  }
  range = (vertices[to_TID]->local_in_edges).equal_range(TID);
  it = range.first;
  while (it != range.second)
  {
    if (it->second == from_TID)
      (vertices[to_TID]->local_in_edges).erase(it++);
    else
      ++it;
  }
}

/*default constructor*/
MVCSR::MVCSR() 
{
  graph= new Graph();
}
/*garbage collection thread that runs as long as gc_active variable is set*/
void* garbage_collector(void *lib)  
{
  while(((MVCSR*)lib)->gc_active)
  {
    //cout<<"gc active\n";
    ((MVCSR*)lib)->gc();
    usleep(((MVCSR*)lib)->gc_interval);
  }
  pthread_exit(NULL);
}
/*starts garbage collection thread*/
void MVCSR::activate_gc()
{
  gc_active=true;
  pthread_t gc_thread = 0;
  pthread_create( &gc_thread, NULL, garbage_collector, (void*)this);
}
/*memory initializer with only data object sizes as parameters*/
int MVCSR::initialize(vector<int> sizes)
{
  activate_gc();
  for(int i=0;i<sizes.size();i++)       
  {
      long long ID;
      int result= create_new(sizes[i],ID);
      if(result!=SUCCESS && result<0)
      return result;
  }
  return SUCCESS;
}

/*memory initializer with data objects' sizes and IDS as parameters*/
int MVCSR::initialize(vector<int> initial_tobs,vector<int> sizes)
{
  activate_gc();
  for(int i=0;i<initial_tobs.size();i++)       
  {
      int result=create_new(initial_tobs[i],sizes[i]);
      if(result!=SUCCESS && result<0)
      return result;
  }
  return SUCCESS;
}

/*default memory initializer*/
int MVCSR::initialize()
{
  activate_gc();
  for(int i=0;i<INITIAL_DATAITEMS;i++)       
  {
      long long ID;
      int result=create_new(ID);
      if(result!=SUCCESS && result<0)
      return result;
  }
  return SUCCESS;
}


/* When ever a trasaction begins, a new vertex is created with corresponding TID and inserted in the graph*/
trans_state* MVCSR::begin()
{
    trans_state* temp=new trans_state;
	  temp->TID=sh_count++;               /*sh_count: atomic variable for allotting the TID*/
    (graph->mtx).lock();
	  graph->add_vertex(temp->TID);
	  (graph->mtx).unlock();
    return temp;
}

/*to end the library*/
void MVCSR::end_library()
{
  gc_active=false;
}

/*
Read function takes as arguments trans_state(contains the TID for transaction) and common_tOB(contains the 
ID,value for a data_item). The user enters the data_item to be read
If the read operation is successful then, value of the data item is entered into tOB argument 
*/

int MVCSR::read(trans_state* trans,common_tOB* tb)
{
  
  cout<<"In read\n";
 //log<<"transaction: "<<trans->TID<<" thread: "<<pthread_self()<<" entered read func and data item is"<<tb->ID<<endl;
  /*search for data item in local buffer*/
  const int id=tb->ID;
  if((trans->local_buffer).find(id)!=(trans->local_buffer).end())
  { /*enters this block if data item is found in local buffer*/
    //cout<<"entered this block\n";
    if(tb->size != ((trans->local_buffer)[tb->ID])->size)
    {
      return SIZE_MISMATCH;  /*sizes of the existing and input data item did not match*/
    }
    if(tb->value==NULL)                /*if the application did not allot memory to input*/
    {
      tb->value=operator new(tb->size);
    }
    copyBytes(tb->value,((trans->local_buffer)[tb->ID])->value,tb->size);          //Returning the value to the user
    return SUCCESS;
  }
  //cout<<"first read\n";
  MVCSR_tOB* tob_temp;
  hash_STM::accessor a_tOB;
  if(sh_tOB_list.find(a_tOB,tb->ID))       /*Finds the data_item in the shared memory and acquires a lock on it*/
    tob_temp=(MVCSR_tOB*)(a_tOB->second);                 
  else
  {
    cout<<"error from read: data object with id= "<<(tb->ID)<<"does not exist"<<endl;
	  a_tOB.release();
    return TOB_ABSENT;                            /*error code 2 indicates that required data object does not exist*/  
  }
  if(tb->size != tob_temp->size)
  {
    cout<<"size mismatch\n";
    a_tOB.release();
    return SIZE_MISMATCH;
  } 

  (graph->mtx).lock();
  /*traverse the list of versions from latest to earliest until a valid version is found (which does not cause cycles)*/
  list<pair<int,MVCSR_version_data*>>::iterator itr=(tob_temp->MVCSR_version_list).end();
  itr--;
  //cout<<"version written by trans "<<itr->first<<endl;
  while(true)
  {
    // cout<<"traversing list\n";
    //add all the the relevant edges formed by reading this version
    //cout<<"cycle forming\n";
    graph->add_local_edge(trans->TID,itr->first,trans->TID);  
    list<pair<int,MVCSR_version_data*>>::iterator v_itr=++itr;
    itr--;
    if(v_itr!=(tob_temp->MVCSR_version_list).end())
    {
      graph->add_local_edge(trans->TID,trans->TID,v_itr->first);
    }
    
    //if cycle is formed, remove all the edges
    set<int> mark_set;
    set<int> visited;
    if(graph->check_cycles(trans->TID,mark_set,visited,trans->TID))
    {
     cout<<"cycle formed\n";
      graph->delete_temp_edge(trans->TID,itr->first,trans->TID);      
      v_itr=++itr;  
      if(v_itr!=(tob_temp->MVCSR_version_list).end())
      {
        graph->delete_temp_edge(trans->TID,trans->TID,v_itr->first);
      }  
    }
    //else break the loop and return the value
    else
    {
      cout<<"cycle not formed\n";
      (tob_temp->read_list).push_back(trans->TID);
      copyBytes(tb->value,((*itr).second)->value,tb->size); /*copying value from shared memory to user input*/ 
      break;   
    }
    (graph->mtx).unlock();
    if(itr==(tob_temp->MVCSR_version_list).begin())
    break;
    itr--;
  }

  a_tOB.release();
  
  local_tOB* local_tob_temp=new local_tOB;               /*updating local buffer*/
  local_tob_temp->ID=tb->ID;
  local_tob_temp->size=tb->size;
  local_tob_temp->value=operator new(tb->size);
  copyBytes(local_tob_temp->value,tb->value,tb->size);
  local_tob_temp->buffer_param=READ_PERFORMED;
  (trans->local_buffer)[tb->ID]=local_tob_temp; 
  ((graph->vertices[trans->TID])->read_set).push_back(tb->ID);
  return SUCCESS;
}


/*delete the current transaction and the vertex representing it*/
void MVCSR::try_abort(trans_state* trans)     
{ 
   cout<<"In abort\n";
   graph->remove_from_readlists(this,trans->TID);
   graph->delete_vertex(trans->TID); 
   delete(trans);
}

/*since write is deferred, we just add writes to local write_buffer*/
int MVCSR::write(trans_state* trans,common_tOB* tb) 
{
  /*search for data item in local buffer*/
  cout<<"In write\n";
  const int id=tb->ID;
  if(trans->local_buffer.find(id)!=(trans->local_buffer).end())
  {
    ((local_tOB*)(trans->local_buffer[tb->ID]))->buffer_param=WRITE_PERFORMED; 
    
    if(tb->size!=(trans->local_buffer[tb->ID])->size)
    return SIZE_MISMATCH;                               /*3 indicates that sizes of existing and required data objects do not match*/
    
    copyBytes((trans->local_buffer[tb->ID])->value,tb->value,tb->size);          /*Returning the value to the user*/
    //cout<<"write successful\n";
    return SUCCESS;
  }

  /*adding new write to local_buffer*/
  local_tOB* tob_temp=new local_tOB;
  tob_temp->ID=tb->ID;
  tob_temp->size=tb->size;
  tob_temp->value=operator new(tb->size);
  copyBytes(tob_temp->value,tb->value,tb->size);
  tob_temp->buffer_param=WRITE_PERFORMED;               
  (trans->local_buffer[tb->ID])=tob_temp;  
  //cout<<"write successful\n";
  return SUCCESS;   
}

/*
 This function takes as arguments trans_state and returns an int depending on the success or failure
 */
 
int MVCSR::try_commit(trans_state* trans,long long& error_tOB)
{     

//log.flush();
//log<<"transaction: "<<trans->TID<<" thread: "<<pthread_self()<<" entered try_commit func"<<endl; 
  cout<<"In commit\n";               
  list<common_tOB*>::iterator tb_itr; 
  list<common_tOB*> write_set;
  map<int,common_tOB*>::iterator hash_itr=(trans->local_buffer).begin();
  for(;hash_itr!=(trans->local_buffer).end();hash_itr++)
  {
    if(((local_tOB*)(hash_itr->second))->buffer_param==WRITE_PERFORMED)   /*write to shared memory only if write was performed by transaction*/
    write_set.push_back(hash_itr->second);    
  }
  if(write_set.size()!=0)
  {
  write_set.sort(compare_local_tOB);                             /*Write buffer is soted based on ID's to impose an order on locking*/
  /* for validation phase we are required to check if lock can be acquired on all data_item in write buffer*/
  hash_STM::accessor a_tOB[write_set.size()];
  int acc_index=0;
  for (tb_itr = (write_set).begin(); tb_itr != (write_set).end(); tb_itr++)   /*Validating whole write buffer*/
  {
    /*check if required data object exists in shared memory*/
    if(!sh_tOB_list.find(a_tOB[acc_index],(*tb_itr)->ID))              /*Acquiring a lock on all data items of write buffer if found*/
    {
      for(int j=0;j<=acc_index;j++)                                /*release all the locks acquired till now before abort*/
      {
        a_tOB[j].release();
	    } 
	    error_tOB=(*tb_itr)->ID; 
	    try_abort(trans);
      return TOB_ABSENT;
    }
    acc_index++;
  }
  cout<<"starting validation\n";
  acc_index=0;
  (graph->mtx).lock();     // acquiring lock on graph
  for (tb_itr = (write_set).begin(); tb_itr != (write_set).end(); tb_itr++)   /*Validating whole write buffer*/
  {
  cout<<"1\n";
    MVCSR_tOB* tob_temp;
	  tob_temp=(MVCSR_tOB*)(a_tOB[acc_index]->second);
	  /*Validating the write operations using MVCSR rule  */
	  /*write-write local edge*/
	  list<pair<int,MVCSR_version_data*>>::iterator v_itr=(tob_temp->MVCSR_version_list).end();
	  v_itr--;
	  graph->add_local_edge(trans->TID,v_itr->first,trans->TID);
  }
  cout<<"successfully added all the edges\n";
  //check for cycles and abort if cycle is formed
  set<int> mark_set;
  set<int> visited;
  if(graph->check_cycles(trans->TID,mark_set,visited,trans->TID))
  {
    cout<<"cycle detected in try commit\n";
    try_abort(trans);
    (graph->mtx).unlock();
    return INVALID_TRANSACTION;
  }
  cout<<"commit successful, making edges permanent\n";
  //else we make the edges permanent and return success
  graph->make_edges_permanent(trans->TID);
  //writing to shared memory
  cout<<"writing to shared memory in try commit\n";
  acc_index=0; 
  for (tb_itr = (write_set).begin(); tb_itr != (write_set).end(); tb_itr++)   
  {
  	/*checking if sizes of data objects in shared memory and write set match*/
	  if((*tb_itr)->size != ((MVCSR_tOB*)(a_tOB[acc_index]->second))->size)    
    {
      for(int j=0;j<write_set.size();j++)                                /*release all the locks acquired till now before abort*/
      {
        a_tOB[j].release();
	    } 
	    error_tOB=(*tb_itr)->ID; 
	    try_abort(trans);
	    (graph->mtx).unlock();
      return SIZE_MISMATCH;                            /*3 indicates that sizes of existing and required data objects do not match*/
    } 
    acc_index++;
  } 
  cout<<"successful till here 1\n";
  /*After validation is successful updating the shared memory with the values stored in write buffer(given by the user)*/
  acc_index=0; 
  for (tb_itr = (write_set).begin(); tb_itr != (write_set).end(); tb_itr++)   
  {
    MVCSR_tOB* tob_temp; 
	  tob_temp=(MVCSR_tOB*)(a_tOB[acc_index]->second);
	  MVCSR_version_data* temp_data=new MVCSR_version_data;
	  void* value=operator new((*tb_itr)->size);
	  temp_data->value=value;
    copyBytes(temp_data->value,(*tb_itr)->value,(*tb_itr)->size);  /*inserting new version into shared memory*/
    pair<int,MVCSR_version_data*> temp_p= make_pair(trans->TID,temp_data);
    (tob_temp->MVCSR_version_list).push_back(temp_p);                     
	  acc_index++;
  }
  for(int j=0;j<write_set.size();j++) 
  {
    a_tOB[j].release();
  }
  }
  int TID=trans->TID;
  delete(trans);     /*freeing memory allocated to trans_state of committing transaction*/
  graph->vertices[TID]->status=COMMITTED;
  (graph->mtx).unlock();
  return SUCCESS; 
}

  
/*creates new data item in shared memory with specified ID,size and default value*/
int MVCSR::create_new(long long ID,int size)   
{
  MVCSR_tOB *tob_temp=new MVCSR_tOB;
  if(!tob_temp)
    return MEMORY_INSUFFICIENT;                     /*unable to create new data object*/
  create_new_lock.lock();                           /*to make data object creation and max_tob_id update operations atomic*/
  hash_STM::const_accessor ca_tOB;
  if(sh_tOB_list.find(ca_tOB,ID))                   /*checking if data object with required id already exists*/
  {
    ca_tOB.release();
    cout<<"Data object with specified ID already exists"<<endl;
    create_new_lock.unlock();
    return TOB_ID_CLASH;                          
  }
  ca_tOB.release();
  tob_temp->ID=ID;
  tob_temp->value=NULL;  
  MVCSR_version_data* temp=new MVCSR_version_data;
  pair<int,MVCSR_version_data*> temp_p= make_pair(0,temp);
  (tob_temp->MVCSR_version_list).push_back(temp_p);
  (temp)->value=operator new(size);     /*transaction t0 writes the first version of every data item*/
  memset((char*)(temp->value),0,size);
  tob_temp->size=size;
  hash_STM::accessor a_tOB; 
  sh_tOB_list.insert(a_tOB,ID);
  a_tOB->second=tob_temp;
  a_tOB.release();
  if(ID>max_tob_id)
  max_tob_id=ID;
  create_new_lock.unlock();            /*releasing mutex lock*/
  return SUCCESS;                           /*returning of newly created data object*/             
} 

/*
creates new data item in shared memory with specified size
*/
int MVCSR::create_new(int size,long long & ID)   
{
                          /*to make data object creation and max_tob_id update operations atomic*/
  MVCSR_tOB *tob_temp=new MVCSR_tOB;
  if(!tob_temp)
    return MEMORY_INSUFFICIENT;                    /*unable to allocate memory to data object*/
  create_new_lock.lock();
  max_tob_id++;                                    /*updating max_tob_id*/
  tob_temp->ID=max_tob_id;                         /*using id=previous max_tob_id+1*/
  tob_temp->value=NULL;
  MVCSR_version_data* temp=new MVCSR_version_data;
  pair<int,MVCSR_version_data*> temp_p= make_pair(0,temp);
  (tob_temp->MVCSR_version_list).push_back(temp_p);
  (temp)->value=operator new(size);     /*transaction t0 writes the first version of every data item*/
  memset((char*)(temp->value),0,size);
  tob_temp->size=size;
  hash_STM::accessor a_tOB; 
  sh_tOB_list.insert(a_tOB,max_tob_id);
  a_tOB->second=tob_temp;
  a_tOB.release();
  ID=max_tob_id;
  create_new_lock.unlock();                      /*releasing mutex lock*/
  return SUCCESS;                             /*returning newly created id*/
}
/*
creates new data object with default size and unused id in shared memory
*/
int MVCSR::create_new(long long & ID)
{
//log.flush();
//log<<" thread: "<<pthread_self()<<" entered create new func"<<endl;
  create_new_lock.lock();                          /*to make data object creation and max_tob_id update operations atomic*/
  MVCSR_tOB *tob_temp=new MVCSR_tOB;
  if(!tob_temp)
    return MEMORY_INSUFFICIENT;                    /*unable to allocate memory to data object*/
  max_tob_id++;                                    /*updating max_tob_id*/
  tob_temp->ID=max_tob_id;                         /*using id=previous max_tob_id+1*/
  tob_temp->size=DEFAULT_DATA_SIZE;
  tob_temp->value=NULL;
  MVCSR_version_data* temp=new MVCSR_version_data;
  temp->value=operator new(DEFAULT_DATA_SIZE);     /*transaction t0 writes the first version of every data item*/
  memset((char*)(temp->value),0,DEFAULT_DATA_SIZE);
  pair<int,MVCSR_version_data*> temp_p= make_pair(0,temp);
  (tob_temp->MVCSR_version_list).push_back(temp_p);
  hash_STM::accessor a_tOB; 
  sh_tOB_list.insert(a_tOB,max_tob_id);
  a_tOB->second=tob_temp;
  a_tOB.release();
  ID=max_tob_id;
  create_new_lock.unlock();                      /*releasing mutex lock*/
  return SUCCESS;                             /*returning newly created id*/
}

/*runs periodically*/
void MVCSR::gc()  
{
  return;
}
