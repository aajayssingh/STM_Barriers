/*
*DESCRIPTION    :   IITH STM transactional barrier.
                    Example application to verify the transactional barriers.
                    g++ -std=gnu++0x -ltbb
                    compatible to c++11 and further c++ versions

*AUTHOR         :   AJAY SINGH
*COMPANY        :   IIT Hyderabad
*/

#include<iostream>
#include "txBarrier.h"
#include <thread>
#include <sstream> //for stringstream

//default values
const int NUM_THREADS = 4;
using namespace std;

int arr[NUM_THREADS] = {0}; //test array to test the functionality of the transactional barrier.

void transaction(int i)
{
    barrier(0);
    for(int indx =0; indx< 100; indx++)
    {
        arr[i]++;
    }

    barrier(1);
    for(int indx =0; indx< 100; indx++)
    {
        arr[i]--;
    }

    barrier(2);
    for(int indx =0; indx< 100; indx++)
    {
        arr[i]++;
    }

    barrier(3);
    for(int indx =0; indx< 100; indx++)
    {
        arr[i]--;
    }
}

int main()
{
    //init the tx barrier initially
    tx_barrier_init(NUM_THREADS);


    /*The test array state prior to the execution of barrier based transaction function*/
    cout<<"test array prior to barrier based execution:"<<endl;
    for (int i = 0; i < NUM_THREADS; ++i)
    {
        cout<<arr[i]<<" ";
    }
    cout<<endl;


    /*Spawn threads to execute the test application named transcation()*/
    thread *t = new thread [NUM_THREADS];
    t = new thread [NUM_THREADS];

    for (int i = 0; i < NUM_THREADS; ++i)
    {
        t[i] = thread(&transaction, i);
    }

    //cout << "main thread\n";

    for (int i = 0; i < NUM_THREADS; ++i)
    {
        t[i].join();
    }


    /*The test array state post execution of barrier based transaction function*/
    cout<<"test array post to barrier based execution:"<<endl;
    for (int i = 0; i < NUM_THREADS; ++i)
    {
        cout<<arr[i]<<" ";
    }
    cout<<endl;

  return 0;
}
