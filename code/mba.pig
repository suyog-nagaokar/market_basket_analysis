AAA = LOAD '$input_file' USING PigStorage(',') AS (dt:chararray, custid:int, amount:double, category:chararray, product:chararray);

AA = FOREACH AAA GENERATE custid as lid, product as item;
AA1 = FOREACH AAA GENERATE custid as lid, product as item;
AA2 = FOREACH AAA GENERATE custid as lid, product as item;

A = DISTINCT AA;
A1 = DISTINCT AA1;
A2 = DISTINCT AA2;

--groups by each item
B = GROUP A BY item ;
B1 = GROUP A1 BY item ;
B2 = GROUP A2 BY item;

--items and frequencies
I_F = FOREACH B GENERATE group, (long)COUNT(A) AS freq;

--filter out items with frequencies that are less than the support
FILTERED_items = FILTER I_F BY (freq >= $min_support_for_pass1);

--Joins DB to itself
C = JOIN A by lid, A1 by lid;
C1 = JOIN A by lid, A1 by lid, A2 by lid;

--Removes the lids
D = FOREACH C GENERATE A::lid as lid, A::item AS leftside, A1::item AS rightside;
D1 = FOREACH C1 GENERATE A::lid as lid, A::item AS leftside, A1::item AS middleside, A2::item AS rightside;

--Keep the tuples that can be used
E = FILTER D BY leftside < rightside;
EE = GROUP E by (leftside,rightside);
cnt = FOREACH EE GENERATE FLATTEN(group.leftside) as leftside ,FLATTEN(group.rightside) as rightside, FLATTEN(COUNT(E.lid)) as item_cnt;

E2 = FILTER D1 BY (leftside < middleside AND middleside < rightside);
EE2 = GROUP E2 by (leftside,middleside,rightside);
cnt2 = FOREACH EE2 GENERATE FLATTEN(group.leftside) as leftside, FLATTEN(group.middleside) as middleside,FLATTEN(group.rightside) as rightside, FLATTEN(COUNT(E2.lid)) as item_cnt2;

--Making 2-itemset Candidates out of 1-itemsets
one = foreach FILTERED_items GENERATE $0;
one1 = foreach FILTERED_items GENERATE $0;
two_itemsets1 = CROSS one,one1;
two_itemsets = DISTINCT two_itemsets1;
filter2s = FILTER two_itemsets BY one::group < one1::group;
two_items = FILTER filter2s BY one::group != one1::group;
two_item = FOREACH two_items GENERATE one::group as lefty, one1::group as righty;

--Finding the 2-itemsets and their frequencies
TEST = JOIN cnt by (leftside,rightside), two_item by (lefty,righty) using 'replicated';
--2-itemsets that meet the support
XX = FOREACH TEST GENERATE cnt::leftside as leftside, cnt::rightside as rightside, cnt::item_cnt as item_cnt;
X = FILTER XX by item_cnt >= $min_support_for_pass2;

--Making 3-itemset candidates out of 2-itemsets
one2 = foreach FILTERED_items GENERATE $0;
three_itemsets1 = CROSS one,one1,one2;
three_itemsets = DISTINCT three_itemsets1;
filter3s = FILTER three_itemsets BY one::group < one1::group AND one1::group < one2::group;
three_items = FILTER filter3s BY one::group != one1::group AND one1::group != one2::group;
three_item = FOREACH three_items GENERATE one::group as lefty, one1::group as middle, one2::group as righty;

--Finding the 3-itemset and their frequencies
TEST2 = JOIN cnt2 by (leftside,middleside,rightside), three_item by (lefty,middle,righty) using 'replicated';
YY = FOREACH TEST2 GENERATE cnt2::leftside as leftside,cnt2::middleside as middleside, cnt2::rightside as rightside, cnt2::item_cnt2 as item_cnt2;
Y = FILTER YY by item_cnt2 >= $min_support_for_pass3;

--Storing the final output--
STORE FILTERED_items into '$pass1_op' using PigStorage(',');
STORE X into '$pass2_op' using PigStorage(',');
STORE Y into '$pass3_op' using PigStorage(',');


