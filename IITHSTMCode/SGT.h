/* 
 * File:   BTO.h
 * Author: Mounika
 *
 * Created on October 27, 2014, 10:59 PM
 */

#ifndef SGT_H
#define	SGT_H
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
  set<long long> read_set;                   // set of tOB IDs read
  set<long long> write_set;                  // set of tOB IDs written
  int status;                                //Specifies if the transaction is active, committed, aborted
  set<int> active_set;
};

/*graph class contains hash map of vertices and methods to edit the graph and check for cycles*/
class Graph {
private:
  map<int,vertex*> vertices;
public:
  Graph();
  virtual ~Graph();
  void add_edge(int,int);
  void add_vertex(int);
  bool check_cycles(int,set<int>,set<int>&);
  void delete_vertex(int);
  friend class SGT;                          //SGT should have access to vertex nodes which are private. So declare it as friend class
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
class SGT : public virtual STM {
private:
   Graph* graph;
   mutex mtx;                           /*lock used for atomic access to graph*/
   mutex create_new_lock;               /*lock used for atomicity of create_new method*/
   
public:
    bool gc_active;                    /*determines whether garbage collection is active or not*/
    SGT();                             /*default constructor*/
    SGT(int freq);                     /*constructor with garbage collection frequency as input*/
    SGT(const SGT& orig);
    virtual ~SGT();
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

#endif	/* SGT_H */

