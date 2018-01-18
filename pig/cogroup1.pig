A = load '/home/hduser/purchase.txt' using PigStorage(',') as (prod:int, pqty:int);
B = load '/home/hduser/sales.txt' using PigStorage(',') as (prod:int, sqty:int);

C = cogroup A by $0, B by $0;
D = foreach C generate group, SUM(A.pqty), SUM(B.sqty);



prod
bag containing data from A
bag containing data from B

cogroup means create ind groups and join them together



prod id, total purchase qty, # of tran, total sales qty, # of sales trans
C = cogroup A by $0, B by $0;
D = foreach C generate group, SUM(A.pqty),COUNT(A), SUM(B.sqty), COUNT(B);


by using co-group
-----------------
txns1.txt and custs

find the total count of transactions, value of those transactions and first name of the customer


John	5	800
Smith	8	700

txn = load 'txns1.txt' using PigStorage(',') as (txnid, txndate, custno:chararray, amount:double, cat, prod, city, state, type);

cust = Load '/home/hduser/custs' using PigStorage(',') as (custno:chararray, firstname:chararray, lastname, age:int, profession:chararray);

txn = foreach txn generate custno, amount;
cust = foreach cust generate custno, firstname;

joined = cogroup cust by $0, txn by $0;

illustrate joined;

(4009999,	{(4009999,Ray)},	

{(4009999,111.47),(4009999,27.08),(4009999,33.06),(4009999,25.24),(4009999,109.87),(4009999,124.63),(4009999,74.67),(4009999,176.0)})


final = foreach joined generate cust.firstname, COUNT(txn), ROUND_TO(SUM(txn.amount),2);












