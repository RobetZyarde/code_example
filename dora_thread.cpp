/*
我的代码并无特别出彩之处，但基本的要求都会做到，努力在修改的同时提高可读性。
每行代码做到简洁，如果有详细的代码规范，我也能学习和遵守将其带入我的代码中。
我认为代码可读性不是一蹴而就的，实在不断维护中提高的。
在维护代码的同时，我将我的构建思路写进了文档中，提供给以后的开发者参考。
*/

#include <tpcc_helper.h>
#include <algorithm>
#include "global.h"
#include "manager.h"
#include "thread.h"
#include "dora_thread.h"
#include "txn.h"
#include "wl.h"
#include "query.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "math.h"
#include "helper.h"
#include "logger.h"
#include "message.h"
#include "abort_queue.h"
#include "maat.h"
#include "ycsb.h"
#include "index_base.h"
#include "index_hash.h"
#include "index_btree.h"
#include "dora_manager.h"
#include "mem_alloc.h"
#include "action_table.h"
#include "sim_manager.h"

#if CC_ALG == DORA

void DORAThread::setup() {
    my_action = NULL;
    txn_man = NULL;
    if (get_thd_id() == 0) {
        send_init_done_to_all_nodes();
    }
}

void DORAThread::init(uint64_t thd_id, uint64_t part_id, ACTION_TYPE atype) {
    my_part_id = part_id;
    my_action_type = atype;
    _thd_id = thd_id;
    _node_id = 0;
}

RC DORAThread::run() {

//2018/3/3 simplify the logic of dora
//TODO: each partition should have its own memory pool for action
//so the memory can keep Single producer single consumer ? (maybe)

#if DORA_NEW
    new_run();
    return FINISH;
#else
    tsetup();
    fflush(stdout);
    printf("Running DORAThread %ld\n", _thd_id);
    std::map<uint64_t,std::list<DoraAction * >>::iterator it;
    std::pair<uint64_t,std::list<DoraAction * >> temp;
    temp.second.push_back(NULL);

    while (!simulation->is_done()) {
        txn_man = NULL;
        heartbeat();
        progress_stats();
        //start with completed queue
        my_action = dora_res_man.dequeue_completed(my_part_id, my_action_type);
        if(my_action != NULL){
            it = my_lock_table.find(my_action->warehouse_id);
            if(it == my_lock_table.end()){
                DEBUG_DORA("FATAL ERROR: LOST THE LOCAL LOCK \n");
            }
            else{
            //run all blocked actions
                if(it->second.size() == 1){
                    //only one lock element
                    //release incoming actions and completed actions
                    action_table.dump((int) dora_res_man.DORA_routing(my_part_id, my_action_type));
                    action_table.dump((int) dora_res_man.DORA_routing(my_part_id, my_action_type) + (int) g_dora_executor_thd_cnt);
                    my_lock_table.erase(it);
                    my_action = NULL;
                    continue;
                }
                else{
                    //has blocked actions
                    //release finished incoming actions and completed actions
                    action_table.dump((int) dora_res_man.DORA_routing(my_part_id, my_action_type));
                    action_table.dump((int) dora_res_man.DORA_routing(my_part_id, my_action_type) + (int) g_dora_executor_thd_cnt);
                    it->second.pop_front();
                    my_action = *(it->second.begin());
                    //run the blocked action
                    txn_man = my_action->txn_man;
                    //txn_man = txn_table.get_transaction_manager(my_action->thd_id, my_action->txn_id, 0);
                    switch(my_action->atype) {
                        case PAYMENT_W:
                            txn_man->run_action_payment_w();
                            phase_0_rvp();
                            break;
                        case PAYMENT_D:
                            txn_man->run_action_payment_d();
                            phase_0_rvp();
                            break;
                        case PAYMENT_C:
                            txn_man->run_action_payment_c();
                            phase_0_rvp();
                            break;
                        case PAYMENT_H:
                            txn_man->run_action_payment_h();
                            dora_res_man.enqueue_completed(my_action->txn_man, my_action->warehouse_id, PAYMENT_W);
                            dora_res_man.enqueue_completed(my_action->txn_man, my_action->warehouse_id, PAYMENT_D);
                            dora_res_man.enqueue_completed(my_action->txn_man, my_action->warehouse_id, PAYMENT_C);
                            dora_res_man.enqueue_completed(my_action->txn_man, my_action->warehouse_id, PAYMENT_H);
                            check_if_done(Commit);
                            break;
                        default:
                            DEBUG_DORA("FATAL ERROR: UNKNOWN ACTION TYPE \n");
                            assert(false);
                    }
                    fflush(stdout);
                    //DEBUG_DORA("TEST: FINISH A BLOCKED ACTION \n");
                    continue; //finish the blocked action
                }
            }
        }

        //if completed queue is empty
        //run incoming queue
        my_action = dora_res_man.dequeue_incoming(my_part_id, my_action_type);
        if(my_action == NULL){
            continue; // incoming queue is empty
        }
        //lock manage
        //TODO: this part is low efficient
        it = my_lock_table.find(my_action->warehouse_id);
        if(it == my_lock_table.end()){
            //nolock
            if(my_action->atype != PAYMENT_H){
                temp.first = my_action->warehouse_id;
                *(temp.second.begin()) = my_action;
                my_lock_table.insert(temp);
            }
            txn_man = my_action->txn_man;
            //txn_man = txn_table.get_transaction_manager(my_action->thd_id, my_action->txn_id, 0);
            switch(my_action->atype) {
                case PAYMENT_W:
                    txn_man->run_action_payment_w();
                    phase_0_rvp();
                    break;
                case PAYMENT_D:
                    txn_man->run_action_payment_d();
                    phase_0_rvp();
                    break;
                case PAYMENT_C:
                    txn_man->run_action_payment_c();
                    phase_0_rvp();
                    break;
                case PAYMENT_H:
                    txn_man->run_action_payment_h();
                    dora_res_man.enqueue_completed(my_action->txn_man, my_action->warehouse_id, PAYMENT_W);
                    dora_res_man.enqueue_completed(my_action->txn_man, my_action->warehouse_id, PAYMENT_D);
                    dora_res_man.enqueue_completed(my_action->txn_man, my_action->warehouse_id, PAYMENT_C);
                    //dora_res_man.enqueue_completed(my_action->thd_id, my_action->txn_id, my_action->warehouse_id, PAYMENT_H);
                    check_if_done(Commit);
                    txn_man = NULL;
                    break;
                default:
                    DEBUG_DORA("FATAL ERROR: UNKNOWN ACTION TYPE \n");
                    assert(false);
            }
            continue; //finish the incoming action
        }
        else{
            //locked
            it->second.push_back(my_action);
            continue;
        }



    }
    return FINISH;
#endif
}

RC DORAThread::new_run(){

//2018/3/7 new design to reduce scheduling overhead
    tsetup();
    fflush(stdout);
    printf("Running DORAThread %ld : %ld : %d \n", _thd_id, my_part_id, (int) my_action_type);

    while (!simulation->is_done()) {

        uint64_t dora_action_dequeue_starttime = get_sys_clock();

        txn_man = NULL;
        heartbeat();
        progress_stats();
        //start with enqueue
        while(!(my_action = dora_res_man.dequeue_incoming(my_part_id,my_action_type)) && !simulation->sim_done){};
        if(!my_action){
            continue;
        }
        txn_man = my_action->txn_man;

        uint64_t dora_action_dequeue_taken_time = get_sys_clock() - dora_action_dequeue_starttime;
        INC_STATS(_thd_id, dora_action_dequeue_time, dora_action_dequeue_taken_time);
        INC_STATS(_thd_id, dora_action_cnt, 1);

        //txn_man = txn_table.get_transaction_manager(my_action->thd_id, my_action->txn_id, 0);
        assert(my_action->atype == my_action_type);

        switch(my_action->atype) {
            case PAYMENT_W:
                {

                    uint64_t dora_action_execute_starttime_W = get_sys_clock();

                    txn_man->run_action_payment_w();
                    //ATOM_ADD(txn_man->Payment_0_rvp_counter, 1);
                    txn_man->Payment_0_rvp_counter.fetch_add(1);
                    action_table.dump((int) dora_res_man.DORA_routing(my_part_id, my_action_type));

                    uint64_t dora_action_execute_taken_time_W = get_sys_clock() - dora_action_execute_starttime_W;
                    INC_STATS(_thd_id, dora_action_execute_time_W, dora_action_execute_taken_time_W);

                    //TODO: timing
                    //DEBUG_DORA("Payment_0_rvp_counter = %d %ld %d \n", txn_man->Payment_0_rvp_counter, my_action->txn_id, action_table.active_cnt[0]);

                    uint64_t dora_action_wait_starttime_W = get_sys_clock();

                    if(txn_man->Payment_0_rvp_counter.load() == 3){
                        txn_man->Payment_1_rvp_counter = 1;
                    }

                    while(!txn_man->Payment_1_rvp_counter && !simulation->sim_done){}

                    uint64_t dora_action_wait_taken_time_W = get_sys_clock() - dora_action_wait_starttime_W;
                    INC_STATS(_thd_id, dora_action_wait_time_W, dora_action_wait_taken_time_W);

                    uint64_t dora_action_execute_starttime_H = get_sys_clock();

                    txn_man->run_action_payment_h();
                    //ATOM_ADD(txn_man->Payment_0_rvp_counter, 1);
                    txn_man->Payment_0_rvp_counter = 4;
                    if(!simulation->sim_done){
                        dora_res_man.enqueue_incoming(my_action->txn_man, my_action->warehouse_id, PAYMENT_H);
                    }

                    uint64_t dora_action_execute_taken_time_H = get_sys_clock() - dora_action_execute_starttime_H;
                    INC_STATS(_thd_id, dora_action_execute_time_H, dora_action_execute_taken_time_H);

                    break;
                }
            case PAYMENT_D:
                {

                    uint64_t dora_action_execute_starttime_D = get_sys_clock();

                    txn_man->run_action_payment_d();
                    //ATOM_ADD(txn_man->Payment_0_rvp_counter, 1);
                    txn_man->Payment_0_rvp_counter.fetch_add(1);
                    action_table.dump((int) dora_res_man.DORA_routing(my_part_id, my_action_type));

                    uint64_t dora_action_execute_taken_time_D = get_sys_clock() - dora_action_execute_starttime_D;
                    INC_STATS(_thd_id, dora_action_execute_time_D, dora_action_execute_taken_time_D);

                    //TODO: timing
                    //DEBUG_DORA("Payment_0_rvp_counter = %d %ld %d \n", txn_man->Payment_0_rvp_counter, my_action->txn_id, action_table.active_cnt);

                    uint64_t dora_action_wait_starttime_D = get_sys_clock();

                    if(txn_man->Payment_0_rvp_counter.load() == 3){
                        txn_man->Payment_1_rvp_counter = 1;
                    }

                    while(!txn_man->Payment_1_rvp_counter && (txn_man->Payment_0_rvp_counter.load() != 4) && !simulation->sim_done){}

                    uint64_t dora_action_wait_taken_time_D = get_sys_clock() - dora_action_wait_starttime_D;
                    INC_STATS(_thd_id, dora_action_wait_time_D, dora_action_wait_taken_time_D);

                    break;
                }
            case PAYMENT_C:

                {

                    uint64_t dora_action_execute_starttime_C = get_sys_clock();

                    txn_man->run_action_payment_c();
                    //ATOM_ADD(txn_man->Payment_0_rvp_counter, 1);
                    txn_man->Payment_0_rvp_counter.fetch_add(1);
                    action_table.dump((int) dora_res_man.DORA_routing(my_part_id, my_action_type));

                    uint64_t dora_action_execute_taken_time_C = get_sys_clock() - dora_action_execute_starttime_C;
                    INC_STATS(_thd_id, dora_action_execute_time_C, dora_action_execute_taken_time_C);

                    //TODO: timing
                    //DEBUG_DORA("Payment_0_rvp_counter = %d %ld %d \n", txn_man->Payment_0_rvp_counter, my_action->txn_id, action_table.active_cnt);

                    uint64_t dora_action_wait_starttime_C = get_sys_clock();

                    if(txn_man->Payment_0_rvp_counter.load() == 3){
                        txn_man->Payment_1_rvp_counter = 1;
                    }

                    while(!txn_man->Payment_1_rvp_counter && (txn_man->Payment_0_rvp_counter.load() != 4) && !simulation->sim_done){}

                    uint64_t dora_action_wait_taken_time_C = get_sys_clock() - dora_action_wait_starttime_C;
                    INC_STATS(_thd_id, dora_action_wait_time_C, dora_action_wait_taken_time_C);

                    break;
                }
            case PAYMENT_H:
                {

                    uint64_t dora_txn_commit_starttime = get_sys_clock();

                    action_table.dump((int) dora_res_man.DORA_routing(my_part_id, my_action_type));
                    //DEBUG_DORA("One transaction committed \n");
                    check_if_done(Commit);

                    uint64_t dora_txn_commit_taken_time = get_sys_clock() - dora_txn_commit_starttime;
                    INC_STATS(_thd_id, dora_txn_commit_time, dora_txn_commit_taken_time);

                    break;
                }
            default:
                DEBUG_DORA("FATAL ERROR: UNKNOWN ACTION TYPE \n");
                assert(false);
        }
    }
    printf("FINISH DORA thread %ld:%ld\n", _node_id, _thd_id);
    fflush(stdout);
    return FINISH;
}


void DORAThread::check_if_done(RC rc) {
//#if !SINGLE_NODE
//    if (txn_man->waiting_for_response())
//        return;
//#endif
    if (rc == Commit)
        commit();
    //TODO: abort logic
    //if (rc == Abort)
        //abort();
}

void DORAThread::commit() {
    assert(txn_man);
//timing
//#if PROFILE_EXEC_TIMING
    //uint64_t timespan = get_sys_clock() - txn_man->txn_stats.starttime;
    //DEBUG_DORA("WT_%ld: COMMIT %ld %f -- %f\n",_thd_id,txn_man->get_txn_id());
//#endif

//#if !SERVER_GENERATE_QUERIES
//    msg_queue.enqueue(_thd_id, Message::create_message(txn_man, CL_RSP), txn_man->client_id);
//#endif
    // remove txn from pool
    txn_man->release_locks(RCOK);
    txn_man->commit_stats();

    uint64_t commit_time = get_sys_clock();
    //uint64_t timespan_short = commit_time - txn_man->txn_stats.restart_starttime;
    uint64_t timespan_long = commit_time - txn_man->txn_stats.starttime;
    //printf("dora_txn_execute_time : %ld\n", timespan_long);
    INC_STATS(_thd_id, dora_txn_execute_time, timespan_long);

    release_txn_man();
}

void DORAThread::phase_0_rvp() {
    //ATOM_ADD(txn_man->Payment_0_rvp_counter, 1);
    txn_man->Payment_0_rvp_counter.fetch_add(1);
    if(txn_man->Payment_0_rvp_counter.load() == 3)
    {
        dora_res_man.enqueue_incoming(my_action->txn_man, my_action->warehouse_id, PAYMENT_H);
        //DEBUG_DORA("ExT_%ld: ENQUEUE HISTORY %ld \n",my_action_type,txn_man->get_txn_id());
        //ATOM_SUB(txn_man->Payment_0_rvp_counter, 3);
        txn_man->Payment_0_rvp_counter.fetch_sub(3);
    }
}

void DORAThread::release_txn_man() {
//   DEBUG_Q("WT_%ld: Releaseing txn man txn_id = %ld\n",_thd_id, txn_man->get_txn_id());
    txn_table.release_transaction_manager(txn_man->get_thd_id(), txn_man->get_txn_id(), txn_man->get_batch_id());
    txn_man = NULL;
}

#endif // CC_ALG == DORA
