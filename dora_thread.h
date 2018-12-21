#ifndef _DORATHREAD_H_
#define _DORATHREAD_H_

#include <tpcc.h>
#include <ycsb.h>
#include "global.h"
#include "txn.h"
#include "tpcc_query.h"
#include <list>
#include <map>
#include "dora_manager.h"

#if CC_ALG == DORA
class Workload;
struct DoraAction;

//DORA Executor thread
class DORAThread : public Thread {
public:
	RC run();
	RC new_run();
	void setup();
	void init(uint64_t thd_id, uint64_t part_id, ACTION_TYPE atype);
	void phase_0_rvp(); //for concensus between different executors
	void commit();
	void check_if_done(RC rc);
	void release_txn_man();

private:
	uint64_t my_part_id;
	ACTION_TYPE my_action_type;
	std::map<uint64_t,std::list<DoraAction * >> my_lock_table; // map <warehouse_id,waiter_list>
	TxnManager * txn_man; //current transaction
	DoraAction * my_action; //current action

};
#endif
#endif