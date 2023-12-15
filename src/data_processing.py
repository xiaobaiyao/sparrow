# data_processing.py

def convert_to_call_pair(step_list, c_list):
    s_c_list = sorted(zip(step_list, c_list))
    contract_list = [x for _, x in s_c_list]
    return contract_list
def process_vip_list(TxnPool, contract_location, n_shards):
    vip_set_list = []
    all_related = [{} for _ in range(n_shards)]
    for tx in TxnPool:
        shard = contract_location[tx.call_graph[0][0]]
        for contract in tx.related_contract:
            if contract_location[contract] != shard:
                if contract in all_related[shard - 1]:
                    all_related[shard - 1][contract] += 1
                else:
                    all_related[shard - 1][contract] = 1
    for d in all_related:
        sorted_dict = dict(sorted(d.items(), key=lambda item: (item[1],item[0]), reverse=True))
        d.clear()
        d.update(sorted_dict)
    for d in all_related:
        num_keys = len(d)
        top_5_percent = int(num_keys * 0.05)
        top_keys = set(list(d.keys())[:top_5_percent])
        vip_set_list.append(top_keys)
        print(len(top_keys))
    print('---------------------------------')
    for i in range(len(vip_set_list)):
        tmp = []
        for kk in vip_set_list[i]:
            if contract_location[kk] == i + 1:
                tmp.append(kk)
        for kk in tmp:
            vip_set_list[i].remove(kk)
    for i in range(1,n_shards + 1):
        print(sum(1 for value in contract_location.values() if value == i))
    print("pre run time: %fs" % (time.time() - time_begin))
    return vip_set_list