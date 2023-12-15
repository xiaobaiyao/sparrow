# Sparrow

Part of Sparrow¡®s state machine is not publicly available at this time due to our patent application. The following is the benchmark source code used in the experiment. In essence, the code in this folder is a simulation of the transaction trace and access control to the database. The actual execution process is run using the state machine modified by the transaction replay tool. This code will show the execution trajectory of the transaction. Due to the large amount of historical transaction data, it will not be included in this file, and users can build their own transaction sets in similar formats according to the code to verify the results.

How to use:
`python sparrow.py shard_number`
Other performance metrics can be appended to the code.
