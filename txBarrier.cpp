/*
*DESCRIPTION    :   IITH STM transactional barrier
*AUTHOR         :   AJAY SINGH
*COMPANY        :   IIT Hyderabad
*/
#include "txBarrier.h"
#include "IITHSTMCode/BTO.cpp"
#include <sstream>
using namespace std;


STM* lib;  /*using BTO protocol of STM library*/
unsigned long int th_num;
void tx_barrier_init(unsigned long int thNum)
{
    th_num = thNum;
    lib=new BTO;  /*using BTO protocol of STM library*/
}

void createId(unsigned long int objid)
{
    int status = lib->create_new(objid, sizeof(int));
    if(status != SUCCESS && status != TOB_ID_CLASH)
    {
        cout<<"failed to create id"<<endl;
    }
}


void barrier(unsigned long int bid)
{

    bool retry = true;
    stringstream msg;
    int trans_succ_flag = FAILURE;
	long long error_id;
	int op_res = FAILURE,read_res=FAILURE;

    createId(bid);

    while(retry)
    {
#if DEBUG_LOGS
        msg<<"retry1"<<endl;
        cout<<msg.str();
        msg.clear();
#endif // DEBUG_LOGS
        //tx to increment the shared counter object
        trans_state* T1 = lib->begin();

        common_tOB* set_obj_p = new common_tOB; //prev tob
        set_obj_p->size = sizeof(int);
        set_obj_p->value = operator new (sizeof(int));
        set_obj_p->ID = bid;
        read_res = lib->read(T1, set_obj_p);
        if(SUCCESS != read_res)//if read failed then Tx is invalid and TS is violated abort
        {
            #if DEBUG_LOGS
            msg<<"tx1 read fail"<<endl;
            cout<<msg.str();
            msg.clear();
            #endif // DEBUG_LOGS
            continue;
        }

        (*((int*)(set_obj_p->value)))=(*((int*)(set_obj_p->value)))+1;
        int tempvalue = (*((int*)(set_obj_p->value)));

        op_res=lib->write(T1,set_obj_p);

        if(op_res == SUCCESS && read_res == SUCCESS)
            trans_succ_flag=lib->try_commit(T1,error_id);

            /*threads can race here so need another transaction to make threads wait*/
        if(trans_succ_flag == SUCCESS)
        {
            retry = false;
            #if DEBUG_LOGS
            msg<<tempvalue/*(*((int*)(set_obj_p->value)))*/<<" TX1 commit success"<<endl;
            cout<<msg.str();
            msg.clear();
            #endif

            if(tempvalue == th_num)
            {
               cout<<"\n\n BANG All arrived*****************\n\n";
            }
        }
        else
        {
            retry = true;
            #if DEBUG_LOGS
            msg<<"TX1 commit fail"<<endl;
            cout<<msg.str();
            msg.clear();
            #endif
        }
    }//while(retry)


    //tx to read the shared counter object to ensure all threads have arrived.
    {

        bool retry2 = true;
        trans_succ_flag = FAILURE;
        read_res=FAILURE;

        while(retry2) //retry untill tx succeeds
        {
#if DEBUG_LOGS
        msg<<"retry2"<<endl;
        cout<<msg.str();
        msg.clear();
#endif // DEBUG_LOGS
            trans_state* T2 = lib->begin();

            common_tOB* set_obj_p = new common_tOB; //prev tob
            set_obj_p->size = sizeof(int);
            set_obj_p->value = operator new (sizeof(int));
            set_obj_p->ID = bid;
            read_res = lib->read(T2, set_obj_p);
            if(SUCCESS != read_res)//if read failed then Tx is invalid and TS is violated abort
            {
                #if DEBUG_LOGS
                msg<<"tx2 read fail"<<endl;
                cout<<msg.str();
                msg.clear();
                #endif // DEBUG_LOGS
                continue;
            }

            int tempvalue = (*((int*)(set_obj_p->value)));

                if(tempvalue < th_num)
                {
                    #if DEBUG_LOGS
                    msg<<"TX2  not all arrived "<<tempvalue<<endl;
                    cout<<msg.str();
                    msg.clear();
                    #endif
                    retry2 = true;
                    continue;
                }
                else
                {
                    #if DEBUG_LOGS
                    msg<<"TX2 awake all by "<<tempvalue<<endl;
                    cout<<msg.str();
                    msg.clear();
                    #endif
                    retry2 = false;
                }


            if(read_res == SUCCESS)
                trans_succ_flag=lib->try_commit(T2,error_id);

                /*threads can race here so need another transaction to make threads wait*/
            if(trans_succ_flag == SUCCESS)
            {
                retry2 = false;
                #if DEBUG_LOGS
                msg<<tempvalue/*(*((int*)(set_obj_p->value)))*/<<"Tx2 commit success"<<endl;
                cout<<msg.str();
                msg.clear();
                #endif

            }
            else
            {
                retry2 = true;
                #if DEBUG_LOGS
                msg<<"TX2 commit fail"<<endl;
                cout<<msg.str();
                msg.clear();
                #endif
            }


        }
    }
}
