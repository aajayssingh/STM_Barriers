/* command to compile class
g++ -std=gnu++0x SGT.cpp -I/home/anila/Desktop/mini/tbb42_20130725oss/include  -Wl,-rpath,/home/anila/Desktop/mini/tbb42_20130725oss/build/linux_intel64_gcc_cc4.6.1_libc2.13_kernel3.0.0_release -L/home/anila/Desktop/mini/tbb42_20130725oss/build/linux_intel64_gcc_cc4.6.1_libc2.13_kernel3.0.0_release  -ltbb
*/

#include "SGT.h"
#include<iostream>
/*constructors and destructors for graph and SGT classes*/
Graph::Graph()
{
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
/*default constructor*/
SGT::SGT() : STM()
{
  graph= new Graph();
}
SGT::SGT(int freq) :STM(freq)
{
  graph= new Graph();
}
SGT::SGT(const SGT& orig)
{
}
SGT::~SGT()                                   /*SGT destructor frees dynamic memory allocated to graph*/
{
  gc_active=false;
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

  /*Delete vertex from hash map*/
  vertices.erase(TID);
}

/*checkfor cycle in graph using DFS algorithm
  Returns true if a cycle is detected and false otherwise*/

bool Graph::check_cycles(int TID,set<int> mark_set,set<int> &visited)
{
  if(mark_set.count(TID))
    return true;
  mark_set.insert(TID);
  list<int>::iterator itr = vertices[TID]->out_edges.begin();
  for(;itr!=vertices[TID]->out_edges.end();itr++)
  {
     /*ignore active transactions except the current transaction that is being checked for correctness*/
     if(visited.count(*itr)||vertices[*itr]->status==ACTIVE)
     continue;
     bool res=check_cycles(*itr,mark_set,visited);
     if(res==true)
     {
       return true;
     }
  }
  visited.insert(TID);
  return false;
}

/*garbage collection thread that runs as long as gc_active variable is set*/
void* garbage_collector(void *lib)
{

  while(((SGT*)lib)->gc_active)
  {
    //cout<<"gc active\n";
    cout<<"GC"<<endl;
    ((SGT*)lib)->gc();
    usleep(((SGT*)lib)->gc_interval);
  }
  pthread_exit(NULL);
}
/*starts garbage collection thread*/
void SGT::activate_gc()
{
  gc_active=true;
  pthread_t gc_thread = 0;
  pthread_create( &gc_thread, NULL, garbage_collector, (void*)this);
}
/*memory initializer with only data object sizes as parameters*/
int SGT::initialize(vector<int> sizes)
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
int SGT::initialize(vector<int> initial_tobs,vector<int> sizes)
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
int SGT::initialize()
{
  activate_gc();
//ajay call initi only after succefully creating all ids
 //@J the use is abused :P only to activate GC
//  for(int i=0;i<INITIAL_DATAITEMS;i++)
//  {
//      long long ID;
//      int result=create_new(ID);
//      if(result!=SUCCESS && result<0)
//      return result;
//  }
  return SUCCESS;
}


/* When ever a trasaction begins, a new vertex is created with corresponding TID and inserted in the graph*/
trans_state* SGT::begin()
{
    trans_state* temp=new trans_state;
    mtx.lock();
	  temp->TID=sh_count++;               /*sh_count: atomic variable for allotting the TID*/
	  graph->add_vertex(temp->TID);
	  mtx.unlock();
    return temp;
}

/*to end the library*/
void SGT::end_library()
{
  gc_active=false;
}

/*adds conflict edges caused due to this read and checks for cycle. If, cycle is formed, transaction will be aborted and 0 is returned
  Else data object is read from shared memory and 1 is returned denoting successful read*/
int SGT::read(trans_state* trans,common_tOB* tb)
{
  mtx.lock();       /*Since graph is edited here, we obtain lock on graph*/
  /*search for data item in local buffer*/
  const int id=tb->ID;
  if((trans->local_buffer).find(id)!=(trans->local_buffer).end())
  {
    if(tb->size != (trans->local_buffer[tb->ID])->size)
    {
      mtx.unlock();
      return SIZE_MISMATCH;
    }
    copyBytes(tb->value,(trans->local_buffer[tb->ID])->value,(trans->local_buffer[tb->ID])->size);          //Returning the value to the user
    mtx.unlock();
    return SUCCESS;
  }
  common_tOB* tob_temp;
  /*Finds the data_item in the shared memory and acquires a lock on it*/
  hash_STM::const_accessor a_tOB;
  if(sh_tOB_list.find(a_tOB,tb->ID))
  tob_temp=a_tOB->second;
  else
  {
    cout<<"error from read: data object with id= "<<(tb->ID)<<"does not exist"<<endl;
	  a_tOB.release();
	  mtx.unlock();
    return TOB_ABSENT;                            /*error code 2 indicates that required data object does not exist*/
  }
  if(tb->size != tob_temp->size)
  {
    a_tOB.release();
    mtx.unlock();
    return SIZE_MISMATCH;
  }
  /*write-read conflicts are detected by iterating over write sets of committed transactions and conflict edges are added*/
  map<int,vertex*>::iterator itr=graph->vertices.begin();
  for(;itr!=graph->vertices.end();itr++)
  {
    if(itr->second->write_set.find(tb->ID)!=itr->second->write_set.end())
      graph->add_edge(itr->second->TID,trans->TID);
  }
  /*checking for existance of cycle*/
  set<int> mark_set;
  set<int> visited;
  graph->vertices[trans->TID]->status=CURRENT;
  bool result=graph->check_cycles(trans->TID,mark_set,visited);
  graph->vertices[trans->TID]->status=ACTIVE;
  /*If cycle exists, release lock on graph and abort the transaction*/
  if(result==true)
  {
     try_abort(trans);
     a_tOB.release();
     mtx.unlock();
     return INVALID_TRANSACTION;
  }
  /*If cycle is not formed, add tOB ID to read set of transaction*/
  graph->vertices[trans->TID]->read_set.insert(tb->ID);

  /*read the data object from shared memory and return its value*/
  if(tb->value==NULL)                /*if the application did not allot memory to input*/
  {
    tb->value=operator new(tob_temp->size);
    tb->size=tob_temp->size;
  }
  copyBytes(tb->value,tob_temp->value,tob_temp->size);   /*Returning the value to the user*/
  a_tOB.release();
  local_tOB* local_tob_temp=new local_tOB;               /*updating local buffer*/
  local_tob_temp->ID=tb->ID;
  local_tob_temp->size=tb->size;
  local_tob_temp->value=operator new(tb->size);
  copyBytes(local_tob_temp->value,tb->value,tb->size);
  local_tob_temp->buffer_param=READ_PERFORMED;
  (trans->local_buffer[tb->ID])=local_tob_temp;
  /*Release the lock on graph*/
  mtx.unlock();
  return SUCCESS;
}

/*delete the current transaction and the vertex representing it*/
void SGT::try_abort(trans_state* trans)
{
   //cout<<"ABORTED\n";
   graph->delete_vertex(trans->TID);
   delete(trans);
}

/*since write is deferred, we just add writes to local write_buffer*/
int SGT::write(trans_state* trans,common_tOB* tb)
{
  /*search for data item in local buffer*/
  const int id=tb->ID;
  if(trans->local_buffer.find(id)!=(trans->local_buffer).end())
  {
    ((local_tOB*)(trans->local_buffer[tb->ID]))->buffer_param=WRITE_PERFORMED;

    if(tb->size!=(trans->local_buffer[tb->ID])->size)
    return SIZE_MISMATCH;                               /*3 indicates that sizes of existing and required data objects do not match*/

    copyBytes((trans->local_buffer[tb->ID])->value,tb->value,tb->size);          /*Returning the value to the user*/
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
  return SUCCESS;
}

/*traverses the write buffer and adds corresponding conflict edges caused due to writes
  If the newly added edges form cycles, transaction is aborted and 0 is returned
  Else transaction status is set to committed and 1 is returned indicating successful commit*/
int SGT::try_commit(trans_state* trans,long long& error_tOB)
{
   mtx.lock();                         /*Obtain lock on graph*/
   list<common_tOB*>::iterator tb_itr;
   list<common_tOB*> write_buffer;
   map<int,common_tOB*>::iterator hash_itr=(trans->local_buffer).begin();
   for(;hash_itr!=(trans->local_buffer).end();hash_itr++)
   {
     if(((local_tOB*)((*hash_itr).second))->buffer_param==WRITE_PERFORMED)   /*write to shared memory only if write was performed by transaction*/
     write_buffer.push_back((*hash_itr).second);
   }
   /*acquire locks on all data items in the order determined by tOB IDs by sorting the write buffer*/
   write_buffer.sort(compare_local_tOB);   /*compare_tOB is defined in STM.h for comparing two tOBs based on their IDs*/
   hash_STM::accessor a_tOB[write_buffer.size()];  /*array of accessors for locking data objects*/
   int acc_index=0;
   for (tb_itr = write_buffer.begin(); tb_itr != write_buffer.end(); tb_itr++)
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
	    mtx.unlock();
      return TOB_ABSENT;
    }
     acc_index++;
   }
   if(!((graph->vertices[trans->TID])->read_set).empty())                              //Optimization check for write only transactions
   {
     for (tb_itr = (write_buffer).begin(); tb_itr != (write_buffer).end(); tb_itr++)   //Validating whole write buffer
     {
       map<int,vertex*>::iterator itr=graph->vertices.begin();
       for(;itr!=graph->vertices.end();itr++)
       {

         vertex* itr_vertex=itr->second;
         if(itr_vertex->status==ACTIVE)  /*update active set for garbage collection*/
           graph->vertices[trans->TID]->active_set.insert(itr_vertex->TID);

         set<long long>::iterator set_itr;
         set_itr=itr_vertex->write_set.find((*tb_itr)->ID);
         if(set_itr!=itr_vertex->write_set.end())          // IF data element being written is present in read set or write set of
           graph->add_edge(itr_vertex->TID,trans->TID);    // other transactions, then add a conflict edge between these transactions
         set_itr=itr_vertex->read_set.find((*tb_itr)->ID);
         if(set_itr!=itr_vertex->read_set.end())
           graph->add_edge(itr_vertex->TID,trans->TID);
       }
     }
     /*check for cycle*/
     set<int> mark_set;
     set<int> visited;
     graph->vertices[trans->TID]->status=CURRENT;
     bool result=graph->check_cycles(trans->TID,mark_set,visited);
     graph->vertices[trans->TID]->status=ACTIVE;
     /*If cycle is found, release lock on graph and abort the transaction*/
     if(result==true)
     {
	     try_abort(trans);
	     /*releasing all acquired locks before return*/
       for(int j=0;j<write_buffer.size();j++)
         a_tOB[j].release();
       mtx.unlock();
       return INVALID_TRANSACTION;
     }
   }
   /*update the data items in shared memory*/
   acc_index=0;
   for (tb_itr = (write_buffer).begin(); tb_itr != (write_buffer).end(); tb_itr++)
   {
     common_tOB* temp;
	   temp=a_tOB[acc_index]->second;
	   /*checking if sizes of data objects in shared memory and write set match*/
	   if((*tb_itr)->size != temp->size)
     {
       for(int j=0;j<write_buffer.size();j++)                                /*release all the locks acquired till now before abort*/
       {
         a_tOB[j].release();
	     }
	     error_tOB=(*tb_itr)->ID;
	     try_abort(trans);
	     mtx.unlock();
       return SIZE_MISMATCH;                            /*3 indicates that sizes of existing and required data objects do not match*/
     }
		 copyBytes(temp->value,(*tb_itr)->value,(*tb_itr)->size);  /*Updating the shared memory with new value*/
		 graph->vertices[trans->TID]->write_set.insert((*tb_itr)->ID);   // update the write set
		 acc_index++;
   }

   /*releasing all acquired locks*/
   for(int j=0;j<write_buffer.size();j++)
     a_tOB[j].release();

   /*deleting local buffer*/
   for (hash_itr = (trans->local_buffer).begin(); hash_itr != (trans->local_buffer).end(); hash_itr++)
   {
     delete(hash_itr->second);
   }
   map<int,common_tOB*>::iterator begin_itr=trans->local_buffer.begin();
   map<int,common_tOB*>::iterator end_itr=trans->local_buffer.end();
   trans->local_buffer.erase(begin_itr,end_itr);
   graph->vertices[trans->TID]->status=COMMITTED;

   /*releasing lock on graph*/
   mtx.unlock();
   return SUCCESS;
}

/*creates new data item in shared memory with specified ID,size and default value*/
int SGT::create_new(long long ID,int size)
{
  common_tOB *tob_temp=new common_tOB;
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
  tob_temp->value=operator new(size);
  memset((char*)(tob_temp->value),0,size);
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
creates new data item in shared memory with specified size and default value
*/
int SGT::create_new(int size,long long& ID)
{
  create_new_lock.lock();                          /*to make data object creation and max_tob_id update operations atomic*/
  common_tOB *tob_temp=new common_tOB;
  if(!tob_temp)
    return MEMORY_INSUFFICIENT;                    /*unable to allocate memory to data object*/
  max_tob_id++;                                    /*updating max_tob_id*/
  tob_temp->ID=max_tob_id;                         /*using id=previous max_tob_id+1*/
  tob_temp->value=operator new(size);
  memset((char*)(tob_temp->value),0,size);
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
int SGT::create_new(long long& ID)
{

  create_new_lock.lock();                          /*to make data object creation and max_tob_id update operations atomic*/
  common_tOB *tob_temp=new common_tOB;
  if(!tob_temp)
    return MEMORY_INSUFFICIENT;                    /*unable to allocate memory to data object*/
  max_tob_id++;                                    /*updating max_tob_id*/
  tob_temp->ID=max_tob_id;                         /*using id=previous max_tob_id+1*/
  tob_temp->value=operator new(DEFAULT_DATA_SIZE);
  memset((char*)(tob_temp->value),0,DEFAULT_DATA_SIZE);
  tob_temp->size=DEFAULT_DATA_SIZE;
  hash_STM::accessor a_tOB;
  sh_tOB_list.insert(a_tOB,max_tob_id);
  a_tOB->second=tob_temp;
  a_tOB.release();
  ID=max_tob_id;
  create_new_lock.unlock();                      /*releasing mutex lock*/
  return SUCCESS;                             /*returning newly created id*/
}

/*runs periodically for every  microseconds*/
void SGT::gc()
{
    mtx.lock();
    map<int,vertex*>::iterator itr;
    set<int>::iterator set_itr;
    itr=graph->vertices.begin();
    for(;itr!=graph->vertices.end();itr++)
    {
      if(itr->second->status==COMMITTED)
      {
        bool can_be_deleted=true;
        set_itr=itr->second->active_set.begin();
        for(;set_itr!=itr->second->active_set.end();set_itr++)
        {
          if((graph->vertices.find(*set_itr)!=graph->vertices.end()))
            if( graph->vertices[*set_itr]->status==ACTIVE)
            {
              can_be_deleted=false;
              break;
            }
        }
        if(can_be_deleted)
        graph->delete_vertex(itr->second->TID);
      }
    }
    mtx.unlock();
}
