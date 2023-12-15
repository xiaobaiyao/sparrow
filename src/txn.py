# transaction.py

class Txnqueue:
    def __init__(self, shard_id):
        self.shard_id = shard_id
        self.txnqueue = []
        self.bias = 0

    def add(self, txn):
        self.txnqueue.append(txn)

    def size(self):
        return len(self.txnqueue)

    def isFull(self):
        return self.size() >= txn_in_blk

class Txn:
    def __init__(self, txn_id, call_graph):
        self.id = txn_id
        self.inter_call_num = len(call_graph)
        self.call_graph = []
        stack = []
        for pair in call_graph:
            if len(stack) == 0:
                stack.append(pair[0])
            else:
                while not pair[0] == stack[-1]:
                    self.call_graph.append((stack[-1], stack[-2]))
                    stack.pop()
            stack.append(pair[1])
            self.call_graph.append(pair)
        while len(stack) >= 2:
            self.call_graph.append((stack[-1], stack[-2]))
            stack.pop()
        remove_txn_id = []
        i = 0
        for pair in self.call_graph:
            if contract_location[pair[0]] == contract_location[pair[1]]:
                remove_txn_id.append(i)
            i += 1
        self.call_graph = [self.call_graph[i] for i in range(len(self.call_graph)) if i == 0 or not i in remove_txn_id]
        self.step = 0
        self.revert_time = 0
        self.lockForShard = set()
        self.begin_time = 0
        self.related_contract = set()
        self.accessed_set = set()
        self.want_access = set()
        for pair in self.call_graph:
            if pair[0] not in self.related_contract:
                self.related_contract.add(pair[0])
            if pair[1] not in self.related_contract:
                self.related_contract.add(pair[1])
        self.want_access = self.related_contract.copy()
        self.inpass = list()
        self.stop_time = 0
        self.max_stop_time = 0

    def isComplete(self):
        return len(self.call_graph) == self.step
