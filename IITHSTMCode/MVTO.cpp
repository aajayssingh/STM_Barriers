#include "MVTO.h"
#include<iostream>
#include <list> 
#include <set>  
using namespace std;
using namespace tbb;

/*default Constructor*/
MVTO::MVTO() : STM()
{
}

/*constructor with garbage collection frequency as parameter*/
MVTO::MVTO(int freq) : STM(freq)
{
}

/*garbage collection thread that runs as long as gc_active variable is set*/
void* garbage_collector(void *lib)  
{
  while(((MVTO*)lib)->gc_active)
  {
    //cout<<"gc active\n";
    ((MVTO*)lib)->gc();
    usleep(((MVTO*)lib)->gc_interval);
  }
  pthread_exit(NULL);
}
/*starts garbage collection thread*/
void MVTO::activate_gc()
{
  gc_active=true;
  pthread_t gc_thread = 0;
  pthread_create( &gc_thread, NULL, garbage_collector, (void*)this);
}
/*memory initializer with only data object sizes as parameters*/
int MVTO::initialize(vector<int> sizes)
{
  activate_gc();
  for(int i=0;i<sizes.size();i++)       
  {
      long long ID;
      int result = create_new(sizes[i],ID);
      if(result!=SUCCESS)
      return result;
  }
  return SUCCESS;
}

/*memory initializer with data objects' sizes and IDS as parameters*/
int MVTO::initialize(vector<int> initial_tobs,vector<int> sizes)
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
int MVTO::initialize()
{
  activate_gc();
  log.flush();
  log.open("log_file.txt",ios_base::out);//ios::out,filebuf::sh_write);
  log<<"initialize successful"<<endl;
  for(int i=0;i<INITIAL_DATAITEMS;i++)       
  {
      long long ID;
      int result=create_new(ID);
      if(result!=SUCCESS)
      return result;
  }
  return SUCCESS;
}

/*
 Begin function creates a new trans_state object and sets the TID value by incrementing atomic 
 variable "sh_count". The trans_state pointer is returned to the user which is used as argument for
 later operations like "read" , "write" , "try_commit" 
*/

trans_state* MVTO::begin()
{
  trans_state* trans_temp=new trans_state;
  trans_temp->TID=sh_count++;						/*incrementing the atomic variable*/
  live_set_lock.lock();
  sh_live_set.insert(trans_temp->TID);
  live_set_lock.unlock(); 
//  log<<"succesfully began transaction "<<trans_temp->TID<<endl;
  return trans_temp;
}
  
/*
Read function takes as arguments trans_state(contains the TID for transaction) and common_tOB(contains the 
ID,value for a data_item). The user enters the data_item to be read
If the read operation is successful then, value of the data item is entered into tOB argument 
*/

int MVTO::read(trans_state* trans,common_tOB* tb)
{
 log<<"transaction: "<<trans->TID<<" thread: "<<pthread_self()<<" entered read func and data item is"<<tb->ID<<endl;
  /*search for data item in local buffer*/
  const int id=tb->ID;
  if((trans->local_buffer).find(id)!=(trans->local_buffer).end())
  { /*enters this block if data item is found in local buffer*/
    if(tb->size != ((trans->local_buffer)[tb->ID])->size)
      return SIZE_MISMATCH;  /*sizes of the existing and input data item did not match*/
    if(tb->value==NULL)                /*if the application did not allot memory to input*/
    {
      tb->value=operator new(tb->size);
    }
    copyBytes(tb->value,((trans->local_buffer)[tb->ID])->value,tb->size);          //Returning the value to the user
    return SUCCESS;
  }
  MVTO_tOB* tob_temp;
  hash_STM::accessor a_tOB;
  if(sh_tOB_list.find(a_tOB,tb->ID))       /*Finds the data_item in the shared memory and acquires a lock on it*/
    tob_temp=(MVTO_tOB*)(a_tOB->second);                 
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
  /*read from latest transaction with time stamp less than current transaction*/
  map<int,MVTO_version_data*>::iterator itr=(tob_temp->MVTO_version_list).lower_bound(trans->TID);
  itr--;
  /*read the data object from shared memory and return its value*/
  if(tb->value==NULL)                /*if the application did not allot memory to input*/
  {
    tb->value=operator new(tob_temp->size);
  }
  if(itr==(tob_temp->MVTO_version_list).end())
  {
    log.flush();
    log<<"error here 1\n";
  }
  else if((itr->second)->value==NULL)
  {
    log.flush();
    log<<"error here 2\n";
  }
  else
  {
    copyBytes(tb->value,(itr->second)->value,tb->size); /*copying value from shared memory to user input*/
    ((itr->second)->read_list).push_back(trans->TID);  /*adding current TID to read list of version read*/
  }
  a_tOB.release();
    
  local_tOB* local_tob_temp=new local_tOB;               /*updating local buffer*/
  local_tob_temp->ID=tb->ID;
  local_tob_temp->size=tb->size;
  local_tob_temp->value=operator new(tb->size);
  copyBytes(local_tob_temp->value,tb->value,tb->size);
  local_tob_temp->buffer_param=READ_PERFORMED;
  (trans->local_buffer)[tb->ID]=local_tob_temp; 
  
  return SUCCESS;
}

 /*
 This function takes as arguments trans_state and returns an int depending on the success or failure
 */
 
int MVTO::try_commit(trans_state* trans,long long& error_tOB)
{     

//log.flush();
//log<<"transaction: "<<trans->TID<<" thread: "<<pthread_self()<<" entered try_commit func"<<endl;                
  list<common_tOB*>::iterator tb_itr; 
  list<common_tOB*> write_set;
  map<int,common_tOB*>::iterator hash_itr=(trans->local_buffer).begin();
  for(;hash_itr!=(trans->local_buffer).end();hash_itr++)
  {
    if(((local_tOB*)(hash_itr->second))->buffer_param==WRITE_PERFORMED)   /*write to shared memory only if write was performed by transaction*/
    write_set.push_back(hash_itr->second);    
  }
  //return SUCCESS;
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
  acc_index=0;
  for (tb_itr = (write_set).begin(); tb_itr != (write_set).end(); tb_itr++)   /*Validating whole write buffer*/
  {
    MVTO_tOB* tob_temp;
	  tob_temp=(MVTO_tOB*)(a_tOB[acc_index]->second);
	  map<int,MVTO_version_data*>::iterator itr;
	  /*Validating the write operations using MVTO rule  */
	  int i_TID=trans->TID;
	  for(itr= (tob_temp->MVTO_version_list).begin() ; itr!=(tob_temp->MVTO_version_list).end() ; itr++)
	  {
	    int j_TID=itr->first;
	    if(i_TID>j_TID)
	    {
	      list<int>::iterator read_list_itr = ((itr->second)->read_list).begin();
	      for(; read_list_itr!=((itr->second)->read_list).end() ; read_list_itr++)
	      {
	        if(i_TID<(*(read_list_itr)))
	        {
	          error_tOB=(*tb_itr)->ID; 
	          try_abort(trans);
	          for(int j=0;j<write_set.size();j++)                                /*release all the locks acquired till now before abort*/
            {
              a_tOB[j].release();
	          } 
            return INVALID_TRANSACTION;
	        }
	      }
	    }
	  }
	  acc_index++;                   
  }
  acc_index=0; 
  for (tb_itr = (write_set).begin(); tb_itr != (write_set).end(); tb_itr++)   
  {
  	/*checking if sizes of data objects in shared memory and write set match*/
	  if((*tb_itr)->size != ((MVTO_tOB*)(a_tOB[acc_index]->second))->size)    
    {
      for(int j=0;j<write_set.size();j++)                                /*release all the locks acquired till now before abort*/
      {
        a_tOB[j].release();
	    } 
	    error_tOB=(*tb_itr)->ID; 
	    try_abort(trans);
      return SIZE_MISMATCH;                            /*3 indicates that sizes of existing and required data objects do not match*/
    } 
    acc_index++;
  } 
  /*After validation is successful updating the shared memory with the values stored in write buffer(given by the user)*/
  acc_index=0; 
  for (tb_itr = (write_set).begin(); tb_itr != (write_set).end(); tb_itr++)   
  {
    MVTO_tOB* tob_temp; 
	  tob_temp=(MVTO_tOB*)(a_tOB[acc_index]->second);
	  MVTO_version_data* temp_data=new MVTO_version_data;
	  void* value=operator new((*tb_itr)->size);
	  temp_data->value=value;
    copyBytes(temp_data->value,(*tb_itr)->value,(*tb_itr)->size);  /*inserting new version into shared memory*/
    (tob_temp->MVTO_version_list)[trans->TID] = temp_data;                     
	  acc_index++;
  }
  for(int j=0;j<write_set.size();j++) 
  {
    a_tOB[j].release();
  }
  }
  live_set_lock.lock();
  sh_live_set.erase(trans->TID);
  live_set_lock.unlock();
  delete(trans);     /*freeing memory allocated to trans_state of committing transaction*/
  return SUCCESS; 
}

 /*Since the MVTO implementation is deferred write, the only job of the protocol is to add to write_set
  which is later validated and updated in the try commit phase */
int MVTO::write(trans_state* trans,common_tOB* tb)
{
log.flush();
log<<"transaction: "<<trans->TID<<" thread: "<<pthread_self()<<" entered write func"<<endl;
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
  /*Aborting the transaction called by all the functions in case of a failure in validation step*/
  
void MVTO::try_abort(trans_state* trans)
{
log.flush();
log<<"transaction: "<<trans->TID<<" thread: "<<pthread_self()<<" entered try_abort func"<<endl;
  live_set_lock.lock();
  sh_live_set.erase(trans->TID);
  live_set_lock.unlock();
  delete(trans);                        /* calls destructor of trans_state which frees the local_buffer*/
}
  
/*creates new data item in shared memory with specified ID,size and default value*/
int MVTO::create_new(long long ID,int size)   
{
  MVTO_tOB *tob_temp=new MVTO_tOB;
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
  MVTO_version_data* temp=new MVTO_version_data;
  (tob_temp->MVTO_version_list)[0]=temp;
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
int MVTO::create_new(int size,long long & ID)   
{
                          /*to make data object creation and max_tob_id update operations atomic*/
  MVTO_tOB *tob_temp=new MVTO_tOB;
  if(!tob_temp)
    return MEMORY_INSUFFICIENT;                    /*unable to allocate memory to data object*/
  create_new_lock.lock();
  max_tob_id++;                                    /*updating max_tob_id*/
  tob_temp->ID=max_tob_id;                         /*using id=previous max_tob_id+1*/
  tob_temp->value=NULL;
  MVTO_version_data* temp=new MVTO_version_data;
  (tob_temp->MVTO_version_list)[0]=temp;
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
int MVTO::create_new(long long & ID)
{
log.flush();
log<<" thread: "<<pthread_self()<<" entered create new func"<<endl;
  create_new_lock.lock();                          /*to make data object creation and max_tob_id update operations atomic*/
  MVTO_tOB *tob_temp=new MVTO_tOB;
  if(!tob_temp)
    return MEMORY_INSUFFICIENT;                    /*unable to allocate memory to data object*/
  max_tob_id++;                                    /*updating max_tob_id*/
  tob_temp->ID=max_tob_id;                         /*using id=previous max_tob_id+1*/
  tob_temp->size=DEFAULT_DATA_SIZE;
  tob_temp->value=NULL;
  MVTO_version_data* temp=new MVTO_version_data;
  temp->value=operator new(DEFAULT_DATA_SIZE);     /*transaction t0 writes the first version of every data item*/
  memset((char*)(temp->value),0,DEFAULT_DATA_SIZE);
  (temp->read_list).push_back(-1);
  (tob_temp->MVTO_version_list)[0]=temp;
  if((tob_temp->MVTO_version_list)[0]->value==NULL)
  log<<"error here 3!\n";
  hash_STM::accessor a_tOB; 
  sh_tOB_list.insert(a_tOB,max_tob_id);
  a_tOB->second=tob_temp;
  a_tOB.release();
  ID=max_tob_id;
  create_new_lock.unlock();                      /*releasing mutex lock*/
  return SUCCESS;                             /*returning newly created id*/
}
 
void MVTO::end_library()
{
  gc_active=false;
  log.close();
}


void MVTO::gc()
{
  set<long long > data_items;
  hash_STM::iterator itr=sh_tOB_list.begin();
  for(;itr!=sh_tOB_list.end();itr++)
  {
    data_items.insert(itr->first);
  }
  set<long long >::iterator d_itr;
  for(d_itr=data_items.begin();d_itr!=data_items.end();d_itr++)
  {
    hash_STM::const_accessor a_tOB;
    if(sh_tOB_list.find(a_tOB,*d_itr))
    {
       map<int,MVTO_version_data*>::iterator v_itr=((MVTO_tOB*)(a_tOB->second))->MVTO_version_list.begin();
       v_itr++;     /*do not consider t0*/
       set<int> to_be_deleted;
       for(;v_itr!=((MVTO_tOB*)(a_tOB->second))->MVTO_version_list.end();v_itr++)
       {
         v_itr++;
         /*if no other transaction has written a newer version for this data item, do not delete it (step 1 of gc)*/
         if(v_itr==((MVTO_tOB*)(a_tOB->second))->MVTO_version_list.end())
         break;
         int k_TID=v_itr->first;
         v_itr--;
         int i_TID=v_itr->first;
         /*verifying step 2 of garbage collection*/
         live_set_lock.lock();
         set<int>::iterator live_itr=sh_live_set.upper_bound(i_TID);
         if(live_itr==sh_live_set.end() || (*live_itr)>k_TID)  //safe to delete version
         {
           to_be_deleted.insert(i_TID);                     //storing the versions to be deleted in a set
         } 
         live_set_lock.unlock();
       }
       set<int>::iterator del_set_itr=to_be_deleted.begin();
       for(;del_set_itr!=to_be_deleted.end();del_set_itr++)   /*deleting all versions in to_be_deleted set for current data item*/
       {
         ((MVTO_tOB*)(a_tOB->second))->MVTO_version_list.erase(*del_set_itr);
       }
       
       a_tOB.release();
    }
  }
} 
MVTO::MVTO(const MVTO& orig) {
}

MVTO::~MVTO() {
}

/*
changes
create t0 transaction
multiple -- for itr
lock versions independently
ask the user for the speed of application to decide the frequency of gc
*/
