def initialize_txn_pool(filename):
    global global_txn_id
    global TxnPool
    global it_has_location
    global where_location
    with pd.read_csv(filename, usecols=['transactionID', 'step', 'from', 'to'],chunksize=2100000) as reader:
        for df in reader:
            chunk_num += 1                       
            df["call"] = df.drop('transactionID', axis=1).drop('step', axis=1).apply(tuple,axis=1)
            df.drop('from', axis=1, inplace=True)
            df.drop('to', axis=1, inplace=True)
            group = df.groupby('transactionID', sort=False)
            group_agg = group.agg(list)
            group_result = group_agg.reset_index()
            for row in group_result.itertuples():
                call_pair_list = convert_to_call_pair(getattr(row, 'step'), getattr(row, 'call'))           
                for pair in call_pair_list:
                    if not pair[0] in it_has_location:
                        it_has_location.add(pair[0])                  
                        where_location[pair[0]] = random.randint(1, n_shards)
                    if not pair[1] in it_has_location:
                        it_has_location.add(pair[1])
                        where_location[pair[1]] = random.randint(1, n_shards)           
                TxnPool.append(Txn(global_txn_id, call_pair_list))
                global_txn_id += 1
    print("There are %d txns in the pool" % len(TxnPool))
