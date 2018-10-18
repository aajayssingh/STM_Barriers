/*
 * File:   FOCC.h
 * Author: Hima Varsha, Raj Kripal
 *
 * Created on October 8, 2015, 1:45 PM
 */

 #ifndef FOCC
 #define	FOCC
 #include "tbb/concurrent_hash_map.h"
 #include "tbb/blocked_range.h"
 #include "tbb/parallel_for.h"
 #include "tbb/tick_count.h"
 #include "tbb/task_scheduler_init.h"
 #include "tbb/tbb_allocator.h"
 #include "STM.cpp"

 using namespace tbb;
 using namespace std;

 /*timestamps: structure which holds the max_read and max_write value of each data item*/
 struct timestamps{
   int max_read;
   int max_write;
 };

 struct FOCC_tOB : public common_tOB
 {
   void* FOCC_data; /*for adding protocol specific data*/
   FOCC_tOB()
   {
     FOCC_data=NULL;
   }
   ~FOCC_tOB()
   {
      delete [] ((char*)FOCC_data);
   }
 };

 /*
   used for local buffer of transactions
   buffer_prameter value 0 => only read
                         1 => written
 */
 struct local_tOB : public FOCC_tOB
 {
   int buffer_param;
 };

 /*Function for comparing tOBs based on their IDs. Used for sorting write_buffer*/
 bool compare_local_tOB (const common_tOB* first, const common_tOB* second)
 {
   return (first->ID < second->ID);
 }


 class FOCC : public virtual STM {
 private:
   mutex create_new_lock;               /*lock used for atomicity of create_new method*/
 public:
     FOCC();                             /*default Constructor*/
     int initialize();                  /*default memory initializer*/
     int initialize(vector<int> sizes); /*takes vector of data object sizes as input and allots memory*/
     int initialize(vector<int> initial_tobs,vector<int> sizes);   /*input => sizes and IDs of initial data objects*/
     FOCC(const FOCC& orig);              /*Copy constructor*/
     virtual ~FOCC();                    /*Destructor*/
     trans_state* begin();
     int read(trans_state* trans,common_tOB* tb)  ;
     int write(trans_state* trans,common_tOB* tb)  ;
     int try_commit(trans_state* trans,long long& error_tOB);
     void try_abort(trans_state* trans);
     int create_new(long long ID,int size) ;
     int create_new(int size,int& ID) ;
     int create_new(int& ID);
 };

 #endif	/* BTO_H */
