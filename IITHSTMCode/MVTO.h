#ifndef MVTO_H
#define	MVTO_H
#include "tbb/concurrent_hash_map.h"
#include "tbb/blocked_range.h"
#include "tbb/parallel_for.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"
#include "STM.cpp"
#include <fstream>
#include <set>
using namespace tbb;
using namespace std;

struct MVTO_version_data
{
  void* value;
  list<int> read_list;
  MVTO_version_data()
  {
    value=NULL;
  }
  ~MVTO_version_data()
  {
     delete [] ((char*)value);
     read_list.clear();
  }
};

struct MVTO_tOB : public common_tOB
{
  map<int,MVTO_version_data*> MVTO_version_list; /*version list for each data item stored using hash map
                                             where key is ID and value is value of data item*/
  ~MVTO_tOB()   /*frees memory allocated to version list*/
  {
    map<int,MVTO_version_data*>::iterator itr=MVTO_version_list.begin();
    for(;itr!=MVTO_version_list.end();itr++)
      delete(itr->second);     /*invokes mvto_version_data destructor*/
    map<int,MVTO_version_data*>::iterator begin_itr=MVTO_version_list.begin();
    map<int,MVTO_version_data*>::iterator end_itr=MVTO_version_list.end();
    MVTO_version_list.erase(begin_itr,end_itr);
  }
};


/*
  used for local buffer of transactions
  buffer_prameter value 0 => only read
                        1 => written
*/
struct local_tOB : public common_tOB
{
  int buffer_param;
};

/*Function for comparing tOBs based on their IDs. Used for sorting write_buffer*/
bool compare_local_tOB (const common_tOB* first, const common_tOB* second)
{
  return (first->ID < second->ID);
}

//typedef concurrent_hash_map<int,bool,MyHashCompare> concurrent_set;
class MVTO : public virtual STM {
private:

  mutex create_new_lock;               /*lock used for atomicity of create_new method*/
  mutex live_set_lock;                 /*lock used for safe access to live set*/
  set<int> sh_live_set;                   /*set of live transactions*/
public:
  std::fstream log;
    bool gc_active;                    /*used to start and stop garbage collection*/
    MVTO();                            /*default Constructor*/
    MVTO(int freq);                    /*constructor with garbage collection frequency as user input*/
    int initialize();                  /*default memory initializer*/
    int initialize(vector<int> sizes); /*takes vector of data object sizes as input and allots memory*/
    int initialize(vector<int> initial_tobs,vector<int> sizes);   /*input => sizes and IDs of initial data objects*/
    MVTO(const MVTO& orig);              /*Copy constructor*/
    virtual ~MVTO();                    /*Destructor*/
    trans_state* begin();
    int read(trans_state* trans,common_tOB* tb)  ;
    int write(trans_state* trans,common_tOB* tb)  ;
    int try_commit(trans_state* trans,long long& error_tOB);
    void try_abort(trans_state* trans);
    int create_new(long long ID,int size) ;
    int create_new(int size,long long& ID) ;
    int create_new(long long& ID);
    void end_library();
    void gc();
    void activate_gc();
};

#endif	/* MVTO_H */
