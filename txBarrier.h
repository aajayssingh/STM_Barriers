/*
*DESCRIPTION    :   IITH STM transactional barrier
*AUTHOR         :   AJAY SINGH
*Institute        :   IIT Hyderabad
*/
#ifndef TX_BARRIER
#define TX_BARRIER
#define DEBUG_LOGS 1

//class txBarrier{
//private:
//    unsigned long int th_num;
//public:
//    txBarrier(unsigned long int th_num);
//    void barrier();
//};

/*
Function to initialize the transactional barrier.
call this at beginnning of the application before multiple threads are spawned.
*/
void tx_barrier_init(unsigned long int th_num);

/*
Barrier which takes unique id as the barrier id. User shall ensure that each barrier call suplies uniquie id.
*/
void barrier(unsigned long int bid);
#endif//TX_BARRIER
