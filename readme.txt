compile >
g++ -std=c++14 -g main.cpp txBarrier.cpp -o test -ltbb -lpthread

The output is verbose as it prints logs. They can be set off by setting DEBUG_LOGS to 0 in txBarrier.h.
The transactions in barrier keep retrying untill they succed. Hence some times it takes lot of time to exit the barrier. 
