
import pandas as pd
import numpy as np
import random
import time
import sys
import matplotlib.pyplot as plt
from data_processing import convert_to_call_pair,process_vip_list
from txn import Txn, Txnqueue
from init_txn_pool import initialize_txn_pool
filename = "myfile.csv"
chunk_num = 0
sim_num = 0
sim_result = 0
n_shards = int(sys.argv[1])
wherelocation = {}
it_has_location = set()
whatlockit = {}
is_has_lock = set()
max_revert_time = 10
txn_has_process = set()
time_begin = time.time()
print("Begin time:", time.asctime(time.localtime(time_begin)))
print("Shard number:", n_shards)
def releaseLock(txn):
    for contract_access in txn.accessed_set:
        if contract_access in is_has_lock:
            is_has_lock.remove(contract_access)
            whatlockit.pop(contract_access)
            patch.add(contract_access)
    for inpass_txn in txn.inpass:
        inpass_txn.lockForShard = set()
    txn.lockForShard = set()
patch = set()   
global_txn_id = 0
TxnPool = []
global_txn_id, TxnPool, it_has_location, wherelocation = initialize_txn_pool(filename)
TxnForShards = [Txnqueue(shard_id) for shard_id in range(1, n_shards + 1)]

mychunksize = 100000
vip_set_list = process_vip_list(TxnPool, wherelocation, n_shards)
all_related = [{} for _ in range(n_shards)]
average_calls = 0
average_function_calls = 0
process_num = 0
txn_in_blk = 100
blk_time = 1 
all_time = 100
txns_for_shards = [[0 for slot in range(all_time)] for i in range(n_shards)]
roll_back_time = 0
abort_time = 0
commit_num = [0 for i in range(5000)]
abort_num = [0 for i in range(5000)]
latency_sum = 0
txn_in_blk_for_shard = [txn_in_blk for i in range(n_shards)]
txn_in_blk_for_shard_next_round = [txn_in_blk for i in range(n_shards)]
complete_txn = [[] for _ in range(n_shards)]
all_related2 = [{} for _ in range(n_shards)]
for slot in range(all_time):
    patch = set()
    rwslot = [0]*n_shards
    bias_list = [0 for _ in range(1,n_shards + 1)]
    for shard_id in range(1, n_shards + 1):
        bias_list[shard_id - 1] = TxnForShards[shard_id - 1].bias
        TxnForShards[shard_id - 1].bias = 0
    for shard_id in range(1, n_shards + 1):
        i = 0
        process_index_in_TxnPool = []
        all_accessed_set = set()
        for txn_id in range(TxnForShards[shard_id - 1].size()):
            all_accessed_set = all_accessed_set.union(TxnForShards[shard_id - 1].txnqueue[txn_id].accessed_set)
        tem_bias = bias_list[shard_id - 1]
        while TxnForShards[shard_id - 1].size() < txn_in_blk + tem_bias and i < len(TxnPool):
            if wherelocation[TxnPool[i].call_graph[0][0]] == shard_id:
                isinpass = False
                TxnPool[i].begin_time = slot - 1
                toremove = []
                tmp_want_access3 = TxnPool[i].want_access.copy()
                for sole_want in tmp_want_access3:
                    if wherelocation[sole_want] == shard_id:
                        toremove.append(sole_want)
                for k in toremove:
                    tmp_want_access3.remove(k)
                for txn_id in range(TxnForShards[shard_id - 1].size()):
                    txn = TxnForShards[shard_id - 1].txnqueue[txn_id]
                    if tmp_want_access3.issubset(vip_set_list[shard_id - 1].union(all_accessed_set)) and len(txn.inpass)<=10:
                        if TxnPool[i].want_access == txn.want_access :
                            TxnForShards[shard_id - 1].txnqueue[txn_id].inpass.append(TxnPool[i])
                            isinpass = True
                            break
                if isinpass != True:
                    TxnForShards[shard_id - 1].add(TxnPool[i])
                process_index_in_TxnPool.append(i)
            i += 1
        TxnPool = [TxnPool[i] for i in range(len(TxnPool)) if not i in process_index_in_TxnPool]
    txn_in_blk_for_shard = txn_in_blk_for_shard_next_round[:]
    txn_in_blk_for_shard_next_round = [txn_in_blk for i in range(n_shards)]
    rollback_txn_id = []
    array = list(range(1, n_shards + 1))
    random.shuffle(array)
    for index, shard_id in enumerate(array):
        bias = bias_list[shard_id - 1]
        all_txn_id = set()
        for seq in range(TxnForShards[shard_id - 1].size()):
            all_txn_id.add(TxnForShards[shard_id - 1].txnqueue[seq].id)
        for txn_id in range(min(txn_in_blk_for_shard[shard_id - 1] + bias, TxnForShards[shard_id - 1].size())):
            txn = TxnForShards[shard_id - 1].txnqueue[txn_id]
            account_lock_by = set()
            account_lock_by.add(txn.id)
            for inpass_txn in txn.inpass:
                account_lock_by.add(inpass_txn.id)
            if txn.step == 0 :
                touch_contract_addr = txn.call_graph[txn.step][0]
                if touch_contract_addr in is_has_lock and \
                    not whatlockit[touch_contract_addr] in account_lock_by:
                    roll_back_time += len(TxnForShards[shard_id - 1].txnqueue[txn_id].inpass) + 1
                    rollback_txn_id.append(TxnForShards[shard_id - 1].txnqueue[txn_id].id)
                    TxnForShards[shard_id - 1].txnqueue[txn_id].stop_time = 0
                    TxnForShards[shard_id - 1].bias -= 1
                else:
                    is_has_lock.add(touch_contract_addr)
                    whatlockit[touch_contract_addr] = txn.id
                    TxnForShards[shard_id - 1].txnqueue[txn_id].lockForShard.add(shard_id)
                    TxnForShards[shard_id - 1].txnqueue[txn_id].accessed_set.add(touch_contract_addr)
                    TxnForShards[shard_id - 1].txnqueue[txn_id].want_access.remove(touch_contract_addr)
                    TxnForShards[shard_id - 1].txnqueue[txn_id].step += 1
            else:
                isbreak = False
                noxiuzheng = False
                round_access_shard = []
                while TxnForShards[shard_id - 1].txnqueue[txn_id].isComplete() != True and isbreak != True:
                    if index >= len(array) / 3:
                        isbreak = True
                        noxiuzheng = True
                    tem_touch_contract = txn.call_graph[TxnForShards[shard_id - 1].txnqueue[txn_id].step][0]
                    if tem_touch_contract in TxnForShards[shard_id - 1].txnqueue[txn_id].accessed_set:
                        if tem_touch_contract not in vip_set_list[shard_id - 1] and wherelocation[tem_touch_contract] != shard_id:
                            isbreak = True
                        TxnForShards[shard_id - 1].txnqueue[txn_id].step += 1
                        tem_lock_shard = wherelocation[tem_touch_contract]
                        if tem_lock_shard not in round_access_shard:
                            TxnForShards[tem_lock_shard - 1].bias -= 1
                            round_access_shard.append(tem_lock_shard)
                    else:
                        if tem_touch_contract not in vip_set_list[shard_id - 1] and wherelocation[tem_touch_contract] != shard_id:
                            isbreak = True
                            if tem_touch_contract in is_has_lock and not whatlockit[tem_touch_contract] in account_lock_by:
                                roll_back_time += len(TxnForShards[shard_id - 1].txnqueue[txn_id].inpass) + 1
                                rollback_txn_id.append(TxnForShards[shard_id - 1].txnqueue[txn_id].id)
                                TxnForShards[shard_id - 1].txnqueue[txn_id].stop_time = 0
                                TxnForShards[shard_id - 1].bias -= 1
                                tem_lock_shard = wherelocation[tem_touch_contract]
                                TxnForShards[tem_lock_shard - 1].bias -= 1
                                to_remove = []
                                for contract_access in TxnForShards[shard_id - 1].txnqueue[txn_id].accessed_set:
                                    if wherelocation[contract_access] == tem_lock_shard:
                                        if contract_access in is_has_lock:
                                            is_has_lock.remove(contract_access)
                                            whatlockit.pop(contract_access)
                                            patch.add(contract_access)
                                            to_remove.append(contract_access)
                                for contract_access in to_remove:
                                    TxnForShards[shard_id - 1].txnqueue[txn_id].accessed_set.remove(contract_access)
                            else:
                                is_has_lock.add(tem_touch_contract)
                                whatlockit[tem_touch_contract] = txn.id
                                TxnForShards[shard_id - 1].txnqueue[txn_id].accessed_set.add(tem_touch_contract)
                                TxnForShards[shard_id - 1].txnqueue[txn_id].want_access.remove(tem_touch_contract)
                                TxnForShards[shard_id - 1].txnqueue[txn_id].step += 1
                                tem_lock_shard = wherelocation[tem_touch_contract]
                                TxnForShards[shard_id - 1].txnqueue[txn_id].lockForShard.add(tem_lock_shard)
                                TxnForShards[tem_lock_shard - 1].bias -= 1
                        else:     
                            if 1==0:
                                path_xiuzheng1 = 1
                                tem_txn1 = TxnForShards[shard_id - 1].txnqueue[txn_id]
                                to_reduce_set1 = set() 
                                while tem_txn1.step + path_xiuzheng1 < len(tem_txn1.call_graph):
                                    to_reduce = tem_txn1.call_graph[tem_txn1.step + path_xiuzheng1][0]
                                    if to_reduce not in vip_set_list[shard_id - 1] and wherelocation[to_reduce] != shard_id:
                                        break
                                    else:
                                        to_reduce_set1.add(to_reduce)
                                    path_xiuzheng1 += 1
                                for to_reduce in to_reduce_set1:    
                                    TxnForShards[wherelocation[to_reduce]-1].bias -= 1
                                to_reduce_set1.clear()
                            elif tem_touch_contract in is_has_lock and not whatlockit[tem_touch_contract] in account_lock_by:
                                if wherelocation[tem_touch_contract] != shard_id and not whatlockit[tem_touch_contract] in all_txn_id:
                                    path_xiuzheng1 = 1
                                    tem_txn1 = TxnForShards[shard_id - 1].txnqueue[txn_id]
                                    to_reduce_set1 = set() 
                                    while tem_txn1.step + path_xiuzheng1 < len(tem_txn1.call_graph):
                                        to_reduce = tem_txn1.call_graph[tem_txn1.step + path_xiuzheng1][0]
                                        if noxiuzheng:
                                            break
                                        if to_reduce not in vip_set_list[shard_id - 1] and wherelocation[to_reduce] != shard_id:
                                            break
                                        else:
                                            to_reduce_set1.add(to_reduce)
                                        path_xiuzheng1 += 1
                                    for to_reduce in to_reduce_set1:    
                                        TxnForShards[wherelocation[to_reduce]-1].bias -= 1
                                    to_reduce_set1.clear()
                                TxnForShards[shard_id - 1].txnqueue[txn_id].stop_time += 1
                                if TxnForShards[shard_id - 1].txnqueue[txn_id].stop_time > TxnForShards[shard_id - 1].txnqueue[txn_id].max_stop_time:
                                    roll_back_time += len(TxnForShards[shard_id - 1].txnqueue[txn_id].inpass) + 1
                                    rollback_txn_id.append(TxnForShards[shard_id - 1].txnqueue[txn_id].id)
                                    TxnForShards[shard_id - 1].txnqueue[txn_id].stop_time = 0
                                    TxnForShards[shard_id - 1].bias -= 1
                                    to_remove = []
                                    tem_lock_shard = wherelocation[tem_touch_contract]
                                    TxnForShards[tem_lock_shard - 1].bias -= 1
                                    for contract_access in TxnForShards[shard_id - 1].txnqueue[txn_id].accessed_set:
                                        if wherelocation[contract_access] == tem_lock_shard:
                                            if 1:
                                                is_has_lock.remove(contract_access)
                                                whatlockit.pop(contract_access)
                                                patch.add(contract_access)
                                                to_remove.append(contract_access)
                                    for contract_access in to_remove:
                                        TxnForShards[shard_id - 1].txnqueue[txn_id].accessed_set.remove(contract_access)
                                isbreak = True
                            else:
                                if tem_touch_contract in patch and wherelocation[tem_touch_contract] != shard_id:
                                    isbreak = True
                                    path_xiuzheng2 = 1
                                    tem_txn2 = TxnForShards[shard_id - 1].txnqueue[txn_id]
                                    to_reduce_set2 = set()
                                    while tem_txn2.step + path_xiuzheng2 < len(tem_txn2.call_graph):
                                        to_reduce = tem_txn2.call_graph[tem_txn2.step + path_xiuzheng2][0]
                                        if noxiuzheng:
                                            break
                                        if to_reduce not in vip_set_list[shard_id - 1] and wherelocation[to_reduce] != shard_id:
                                            break
                                        else:
                                            to_reduce_set2.add(to_reduce)
                                        path_xiuzheng2 += 1
                                    for to_reduce in to_reduce_set2:    
                                        TxnForShards[wherelocation[to_reduce]-1].bias -= 1
                                    to_reduce_set2.clear()
                                is_has_lock.add(tem_touch_contract)
                                whatlockit[tem_touch_contract] = txn.id
                                TxnForShards[shard_id - 1].txnqueue[txn_id].accessed_set.add(tem_touch_contract)
                                TxnForShards[shard_id - 1].txnqueue[txn_id].want_access.remove(tem_touch_contract)
                                TxnForShards[shard_id - 1].txnqueue[txn_id].step += 1
                                tem_lock_shard = wherelocation[tem_touch_contract]
                                TxnForShards[shard_id - 1].txnqueue[txn_id].lockForShard.add(tem_lock_shard)
                                TxnForShards[tem_lock_shard - 1].bias -= 1
            if TxnForShards[shard_id - 1].txnqueue[txn_id].isComplete():
                TxnForShards[shard_id - 1].bias -= 1
                tmp_lockForShard = set()
                for lock_shard_id in TxnForShards[shard_id - 1].txnqueue[txn_id].lockForShard:
                    tmp_lockForShard.add(lock_shard_id)
                for inpass_txn in TxnForShards[shard_id - 1].txnqueue[txn_id].inpass:
                    for lock_shard_id in inpass_txn.lockForShard:
                        tmp_lockForShard.add(lock_shard_id)
                for lock_shard_id in tmp_lockForShard:    
                    txn_in_blk_for_shard_next_round[lock_shard_id - 1] = max(0, txn_in_blk_for_shard_next_round[lock_shard_id - 1] - 1)
                to_remove = []
                for contract_access in TxnForShards[shard_id - 1].txnqueue[txn_id].accessed_set:
                    if wherelocation[contract_access] == shard_id:
                        if 1:
                            is_has_lock.remove(contract_access)
                            whatlockit.pop(contract_access)
                            patch.add(contract_access)
                            to_remove.append(contract_access)
                for contract_access in to_remove:
                    TxnForShards[shard_id - 1].txnqueue[txn_id].accessed_set.remove(contract_access)
                complete_txn[shard_id - 1].append(TxnForShards[shard_id - 1].txnqueue[txn_id])
                for txn_inpass in TxnForShards[shard_id - 1].txnqueue[txn_id].inpass:
                    tem_shard = wherelocation[txn_inpass.call_graph[0][0]]
                    complete_txn[tem_shard - 1].append(txn_inpass)
            TxnForShards[shard_id - 1].bias += 1
        for txn_id in range(TxnForShards[shard_id - 1].size()):
            if TxnForShards[shard_id - 1].txnqueue[txn_id].isComplete() or TxnForShards[shard_id - 1].txnqueue[txn_id].id in rollback_txn_id:
                releaseLock(TxnForShards[shard_id - 1].txnqueue[txn_id])
    for shard_id in range(1, n_shards + 1):
        abort_txn_id = []
        tmp_txn_for_shard = []
        for txn_id in range(TxnForShards[shard_id - 1].size()):
            if TxnForShards[shard_id - 1].txnqueue[txn_id].id in rollback_txn_id:
                tmp_lockForShard = set()
                for lock_shard_id in TxnForShards[shard_id - 1].txnqueue[txn_id].lockForShard:
                    tmp_lockForShard.add(lock_shard_id)
                for inpass_txn in TxnForShards[shard_id - 1].txnqueue[txn_id].inpass:
                    for lock_shard_id in inpass_txn.lockForShard:
                        tmp_lockForShard.add(lock_shard_id)
                for lock_shard_id in tmp_lockForShard:    
                    txn_in_blk_for_shard_next_round[lock_shard_id - 1] = max(0, txn_in_blk_for_shard_next_round[lock_shard_id - 1] - 1)
                TxnForShards[shard_id - 1].txnqueue[txn_id].step = 0
                TxnForShards[shard_id - 1].txnqueue[txn_id].revert_time += 1
                TxnForShards[shard_id - 1].txnqueue[txn_id].want_access = TxnForShards[shard_id - 1].txnqueue[txn_id].related_contract.copy()
                TxnForShards[shard_id - 1].txnqueue[txn_id].accessed_set = set()
                for inpass_txn in TxnForShards[shard_id - 1].txnqueue[txn_id].inpass:
                    inpass_txn.step = 0
                    inpass_txn.want_access = inpass_txn.related_contract.copy()
                    inpass_txn.revert_time += 1
                    inpass_txn.accessed_set = set()
                    if inpass_txn.revert_time > max_revert_time:
                        abort_txn_id.append(inpass_txn.id)
                        abort_time += 1
                        abort_num[inpass_txn.inter_call_num] += 1
                    else:
                        tmp_txn_for_shard.append(inpass_txn)
                TxnForShards[shard_id - 1].txnqueue[txn_id].inpass.clear()
                if TxnForShards[shard_id - 1].txnqueue[txn_id].revert_time > max_revert_time:
                    abort_txn_id.append(txn_id)
                    abort_time += 1
                    abort_num[TxnForShards[shard_id - 1].txnqueue[txn_id].inter_call_num] += 1
        TxnForShards[shard_id - 1].txnqueue = [TxnForShards[shard_id - 1].txnqueue[txn_id] for txn_id in range(TxnForShards[shard_id - 1].size()) if not txn_id in abort_txn_id]
        TxnForShards[shard_id - 1].txnqueue.extend(tmp_txn_for_shard)
        finished_txn_id = []
        for txn_id in range(TxnForShards[shard_id - 1].size()):
            if TxnForShards[shard_id - 1].txnqueue[txn_id].isComplete():
                finished_txn_id.append(txn_id)
                if TxnForShards[shard_id - 1].txnqueue[txn_id].transactionID in rwdic:
                    for shard_num in range(1, n_shards + 1):
                        for rw_key,slot_num in rwdic[TxnForShards[shard_id - 1].txnqueue[txn_id].transactionID].items():
                            if rw_key in vip_set_list[shard_num - 1]:
                                rwslot[shard_num - 1] += slot_num
                average_function_calls += TxnForShards[shard_id - 1].txnqueue[txn_id].inter_call_num
                average_calls += len(TxnForShards[shard_id - 1].txnqueue[txn_id].call_graph)
                process_num += 1
                txns_for_shards[shard_id - 1][slot] += 1
                latency_sum += slot - TxnForShards[shard_id - 1].txnqueue[txn_id].begin_time
                commit_num[TxnForShards[shard_id - 1].txnqueue[txn_id].inter_call_num] += 1
                for inpass_txn in TxnForShards[shard_id - 1].txnqueue[txn_id].inpass:
                    if inpass_txn.transactionID in rwdic:
                        for shard_num in range(1, n_shards + 1):
                            for rw_key,slot_num in rwdic[inpass_txn.transactionID].items():
                                if rw_key in vip_set_list[shard_num - 1]:
                                    rwslot[shard_num - 1] += slot_num
                    process_num += 1
                    average_function_calls += inpass_txn.inter_call_num
                    average_calls += len(inpass_txn.call_graph)
                    latency_sum += slot - inpass_txn.begin_time
                    commit_num[inpass_txn.inter_call_num] += 1
                    txns_for_shards[shard_id - 1][slot] += 1
        TxnForShards[shard_id - 1].txnqueue = [TxnForShards[shard_id - 1].txnqueue[txn_id] for txn_id in range(TxnForShards[shard_id - 1].size()) if not txn_id in finished_txn_id]
        txns_want_to_inpass = []
        all_accessed_set = set()
        for txn_id in range(TxnForShards[shard_id - 1].size()):
            all_accessed_set = all_accessed_set.union(TxnForShards[shard_id - 1].txnqueue[txn_id].accessed_set)
        for txn_id in range(TxnForShards[shard_id - 1].size()):
            if txn_id == 0:
                continue
            else:
                tmp_want_access = TxnForShards[shard_id - 1].txnqueue[txn_id].want_access
                toremove = []
                tmp_want_access2 = tmp_want_access.copy()
                for sole_want in tmp_want_access2:
                    if wherelocation[sole_want] == shard_id:
                        toremove.append(sole_want)
                for i in toremove:
                    tmp_want_access2.remove(i)
                if tmp_want_access2.issubset(vip_set_list[shard_id - 1].union(all_accessed_set)):
                    for engine_txn_id in range(txn_id):
                        if engine_txn_id in txns_want_to_inpass or len(TxnForShards[shard_id - 1].txnqueue[engine_txn_id].inpass) > 10:
                            continue
                        else:
                            engine_txn_want_access = TxnForShards[shard_id - 1].txnqueue[engine_txn_id].want_access
                            engine_txn_accessed = TxnForShards[shard_id - 1].txnqueue[engine_txn_id].accessed_set
                            if tmp_want_access == engine_txn_want_access:
                                TxnForShards[shard_id - 1].txnqueue[engine_txn_id].inpass.append(TxnForShards[shard_id - 1].txnqueue[txn_id])
                                TxnForShards[shard_id - 1].txnqueue[engine_txn_id].lockForShard.update(TxnForShards[shard_id - 1].txnqueue[txn_id].lockForShard)
                                TxnForShards[shard_id - 1].txnqueue[engine_txn_id].accessed_set.update(TxnForShards[shard_id - 1].txnqueue[txn_id].accessed_set)
                                txns_want_to_inpass.append(txn_id)
                                break
        TxnForShards[shard_id - 1].txnqueue = [TxnForShards[shard_id - 1].txnqueue[txn_id] for txn_id in range(TxnForShards[shard_id - 1].size()) if not txn_id in txns_want_to_inpass]
    print(sum(rwslot)/len(rwslot))
print("There are %d txns in the pool (after processing)" % len(TxnPool))
txns_for_shards_sum = [sum(i) for i in txns_for_shards]
txns_for_shards_sum_sum = sum(txns_for_shards_sum)
average_calls /= process_num
average_function_calls /= process_num
ratio = [0 for i in range(5000)]
