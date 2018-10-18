#include"STM.cpp"
#include "tbb/concurrent_hash_map.h"
#include "tbb/blocked_range.h"
#include "tbb/parallel_for.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"
#include<set>
#include<list>
/*macros used to set the status of transaction*/
#define ABORTED 0
#define ACTIVE 1
#define COMMITTED 2 
#define CURRENT 3


using namespace tbb;
using namespace std;
/*each vertex in graph corresponds to a transaction*/
struct vertex {     
  int TID;                                   // TID of corresponding transaction
  list<int> out_edges;                       // list of outgoing edges from current node
  list<int> in_edges;                        // list of incoming edges to current node
  list<long long> read_set;                   // set of tOB IDs read
  set<long long> write_set;                  // set of tOB IDs written
  int status;                                //Specifies if the transaction is active, committed, aborted
  multimap<int,int> local_out_edges;              // key-> TID causing this edge, value -> to which TID the edge is outgoing
  multimap<int,int> local_in_edges;               // key-> TID causing this edge, vallue-> from which tid this edge is coming
  multiset<int> effected_transactions;                 // set of transactions where edges are caused due to current transaction
};

/*graph class contains hash map of vertices and methods to edit the graph and check for cycles*/
class Graph {
private:
  map<int,vertex*> vertices;
public:
  Graph();
  virtual ~Graph();
  mutex mtx;                           /*lock used for atomic access to graph*/
  void add_edge(int,int);
  void add_vertex(int);
  bool check_cycles(int,set<int>,set<int>&,int);
  void delete_vertex(int);
  void make_edges_permanent(int);
  void add_local_edge(int,int,int);
  void delete_temp_edge(int,int,int);
  void insert_effected(int,int);
  void delete_effected(int,int);
  void remove_from_readlists(STM*,int);
/*  void insert_readset(int,int);
  void set_status(int);
  int get_status(int);*/
  friend class MVCSR;                          //SGT should have access to vertex nodes which are private. So declare it as friend class
};


struct MVCSR_version_data
{
  void* value;
  MVCSR_version_data()
  {
    value=NULL;
  }
  ~MVCSR_version_data()
  {
     delete [] ((char*)value);
  }
};

struct MVCSR_tOB : public common_tOB
{
  list<pair<int,MVCSR_version_data*>> MVCSR_version_list; /*version list for each data item stored using list of pairs 
                                                         where pair consists of key (TID), value of the data item*/
  list<int> read_list;
  ~MVCSR_tOB()   /*frees memory allocated to version list*/
  {
    list<pair<int,MVCSR_version_data*>>::iterator itr=MVCSR_version_list.begin();
    for(;itr!=MVCSR_version_list.end();itr++)
      delete(itr->second);     /*invokes mvto_version_data destructor*/
    //delete list
    //delete read list
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

/*SGT protocol implements all the virtual methods of STM library*/
class MVCSR : public virtual STM {
private:
   Graph* graph;
   mutex create_new_lock;               /*lock used for atomicity of create_new method*/
   
public:
    bool gc_active;                    /*determines whether garbage collection is active or not*/
    MVCSR();
    MVCSR(const MVCSR& orig);
    virtual ~MVCSR();
    int initialize();                  /*default memory initializer*/
    int initialize(vector<int> sizes); /*takes vector of data object sizes as input and allots memory*/
    int initialize(vector<int> initial_tobs,vector<int> sizes);   /*input => sizes and IDs of initial data objects*/
    trans_state* begin();
    int read(trans_state* trans,common_tOB* tb);
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


/*
  activate gc private (not possible)
  graph.cpp
  remove friend class by writing functions
  inherit sgt;mvcsr 
  reduce redirection
  
  move lock to graph
  why is set used for efffected_transactions?
  constructor for mvcsr_version_data
  mvcsr_tob : make list private and add functions??
  move create_new lock to STM (since it is for shared memory)
  sgt line 127 (concurrency issues with gc_active??)
  list of effected edges
  line 134, ACTIVE 
  
  implement both versions: with out local opacity and adding write write edges and compare them
  sh_count++ -> function get_tid
  read:: obtain lock later
  write-write edges is read are not required
*/
