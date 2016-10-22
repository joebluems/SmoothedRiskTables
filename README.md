# SmoothedRiskTables
Using Apache Drill to generate Smoothed Risk tables for assorted Loan Products

## Get Apache Drill
Link for the download: http://drill.apache.org/download/ <BR>
Follow instructions to setup (i.e. JAVA_HOME) and install Drill <BR>
Launch drill from the main drill folder with this command: <b> ./bin/drill-embedded </b> <BR>
<BR>
## Setup a workspace
To write a risk table you need to create a schema in the dfs workspace. <BR>
Open up a browser and go to http://localhost:8047/<BR>
From the Storage menu, select Update for the dfs storage plugin.<BR> 
Create a new schema called risk. On my Mac, it looks like this:<BR>
<BR>
"risk": {<BR>
      "location": "/Users/joeblue/RiskTables/tables",<BR>
      "writable": true,<BR>
      "defaultInputFormat": null<BR>
    }<BR>
<BR>
You must create the folder <b>tables</b> in the location above to complete the workspace. When selecting Update, you should get a “success” message. <BR>
If you already started Drill, exit the shell (i.e. <ctrl-D>) and restart it to be able to access the risk schema. <BR>
<BR>
The Drill Web Console is where you can connect Drill to a variety of other tools and further configure your workspaces. For more on topics, check out these links:<BR>
Plugins - http://drill.apache.org/docs/storage-plugin-configuration/<BR>
Workspaces - http://drill.apache.org/docs/workspaces/<BR>
<BR>

## Building Risk Tables for Loan Types
For this example, we are attempting to create a smoothed risk table for loan types based on whether or not a customer is a “good” account. The label of “good” could be based on proclivity against churn or an extra-profitable type of customer persona or segment.<BR>
<BR>
This data was simulated with the python script <b>data.py</b> to create the file <b>loanType.json</b>. Use your own data or simulate some new data with the script. Make sure you change the file location as needed to avoid a file-not-found error.<BR>

All of the Drill queries and commands below should work as copy-paste. Tips:<BR>
1)	Tick marks are important (make sure your system doesn’t convert to quotes)<BR>
2)	When copying, don’t include the drill prompt (i.e. <b>0: jdbc:drill:> <b>)<BR>
<BR>
Explore the <b>loanType.json</b> file inside Drill:<BR>
<BR>
```
0: jdbc:drill:> select * from dfs.`/Users/joeblue/RiskTables/loanType.json` limit 5;
+-------+-----------+-------+
| acct  | loanType  | good  |
+-------+-----------+-------+
| 0     | 8         | 1     |
| 0     | 10        | 1     |
| 0     | 5         | 1     |
| 0     | 4         | 1     |
| 0     | 7         | 1     |
+-------+-----------+-------+
```
<BR>
Count the number of customer loans: <BR>
```
0: jdbc:drill:> select count(*) from dfs.`/Users/joeblue/RiskTables/loanType.json`;
+---------+
| EXPR$0  |
+---------+
| 500086  |
+---------+
```
This query shows the number of customers with each loan and sorts by descending count:<BR>
```
0: jdbc:drill:> select loanType,count(*) `count` from dfs.`/Users/joeblue/RiskTables/loanType.json`
group by loanType
order by count(*) desc;
+-----------+--------+
| loanType  | count  |
+-----------+--------+
| 0         | 65347  |
| 2         | 64349  |
| 1         | 63450  |
| 3         | 54468  |
| 5         | 53639  |
| 4         | 50233  |
| 6         | 42753  |
| 8         | 37018  |
| 7         | 34746  |
| 10        | 16803  |
| 9         | 9996   |
| 11        | 7284   |
+-----------+--------+
```
Calculate the total number of “good” accounts and “other” accounts (over the entire population). This ratio tells us the mean good rate = numGood/(numGood + numOther).  The relative risks we calculate will be based on an increase from this rate (more likely to be good) vs. decrease (less likely to be good) for each loan type. The dummy field is for merging in the next query. <BR>
```
0: jdbc:drill:zk=local> select 1 as `dummy`,sum(totalGood) `numGood`,sum(totalOther) `numOther` from (
select case when good='1' then 1 else 0 end as `totalGood`,
case when good='0' then 1 else 0 end as `totalOther`
from ( select distinct acct,good from dfs.`/Users/joeblue/RiskTables/loanType.json`)
);
+--------+----------+-----------+
| dummy  | numGood  | numOther  |
+--------+----------+-----------+
| 1      | 25000    | 75000     |
+--------+----------+-----------+
```
Next, calcualte the total Goods and Others for each loan type using a case statement and grouping by loan type: <BR>
```
0: jdbc:drill:> select 1 as `dummy`,loanType,sum(goods) `goods`,sum(others) `others` from (
select loantype,
case when good='1' then 1 else 0 end as `goods`,
case when good='0' then 1 else 0 end as `others`
from ( select loantype,good from dfs.`/Users/joeblue/RiskTables/loanType.json`)
) group by loanType;
+--------+-----------+--------+---------+
| dummy  | loanType  | goods  | others  |
+--------+-----------+--------+---------+
| 1      | 8         | 8331   | 28687   |
| 1      | 10        | 9940   | 6863    |
| 1      | 5         | 14393  | 39246   |
| 1      | 4         | 9385   | 40848   |
| 1      | 7         | 5138   | 29608   |
| 1      | 2         | 17470  | 46879   |
| 1      | 0         | 16446  | 48901   |
| 1      | 6         | 11801  | 30952   |
| 1      | 1         | 16296  | 47154   |
| 1      | 3         | 13305  | 41163   |
| 1      | 11        | 1139   | 6145    |
| 1      | 9         | 1103   | 8893    |
+--------+-----------+--------+---------+
```
Join the two pieces together – the data is now prepared to calculate risk: <BR>
```
select * from (
select 1 as `dummy`,loanType,sum(goods) `goods`,sum(others) `others` from
(
select loantype,case when good='1' then 1 else 0 end as `goods`,
case when good='0' then 1 else 0 end as `others`
from ( select loantype,good from dfs.`/Users/joeblue/RiskTables/loanType.json`)
) group by loanType ) `LOAN`
JOIN
( select 1 as `dummy`,sum(totalGood) `numGood`,sum(totalOther) `numOther` from (
select case when good='1' then 1 else 0 end as `totalGood`,
case when good='0' then 1 else 0 end as `totalOther`
from ( select distinct acct,good from dfs.`/Users/joeblue/RiskTables/loanType.json`)) 
)`TOTAL`
ON LOAN.dummy=TOTAL.dummy;
+--------+-----------+--------+---------+---------+----------+-----------+
| dummy  | loanType  | goods  | others  | dummy0  | numGood  | numOther  |
+--------+-----------+--------+---------+---------+----------+-----------+
| 1      | 8         | 8331   | 28687   | 1       | 25000    | 75000     |
| 1      | 10        | 9940   | 6863    | 1       | 25000    | 75000     |
| 1      | 5         | 14393  | 39246   | 1       | 25000    | 75000     |
| 1      | 4         | 9385   | 40848   | 1       | 25000    | 75000     |
| 1      | 7         | 5138   | 29608   | 1       | 25000    | 75000     |
| 1      | 2         | 17470  | 46879   | 1       | 25000    | 75000     |
| 1      | 0         | 16446  | 48901   | 1       | 25000    | 75000     |
| 1      | 6         | 11801  | 30952   | 1       | 25000    | 75000     |
| 1      | 1         | 16296  | 47154   | 1       | 25000    | 75000     |
| 1      | 3         | 13305  | 41163   | 1       | 25000    | 75000     |
| 1      | 11        | 1139   | 6145    | 1       | 25000    | 75000     |
| 1      | 9         | 1103   | 8893    | 1       | 25000    | 75000     |
+--------+-----------+--------+---------+---------+----------+-----------+
```
Create the basic (i.e. “vanilla”) risk table: <BR>
```
select LOAN.*,TOTAL.numGood,TOTAL.numOther,
(cast(LOAN.goods as float)/(LOAN.goods+LOAN.others)/TOTAL.numGood*(TOTAL.numGood+TOTAL.numOther)) `risk`  
from (
select 1 as `dummy`,loanType,sum(goods) `goods`,sum(others) `others` from
(
select loantype,case when good='1' then 1 else 0 end as `goods`,
           case when good='0' then 1 else 0 end as `others`
           from ( select loantype,good from dfs.`/Users/joeblue/RiskTables/loanType.json`)
) group by loanType ) `LOAN`
JOIN
( select 1 as `dummy`,sum(totalGood) `numGood`,sum(totalOther) `numOther` from (
 select case when good='1' then 1 else 0 end as `totalGood`,
           case when good='0' then 1 else 0 end as `totalOther`
           from ( select distinct acct,good from dfs.`/Users/joeblue/RiskTables/loanType.json`)) 
)`TOTAL`
ON LOAN.dummy=TOTAL.dummy
order by loanType;
+--------+-----------+--------+---------+----------+-----------+-------------+
| dummy  | loanType  | goods  | others  | numGood  | numOther  |    risk     |
+--------+-----------+--------+---------+----------+-----------+-------------+
| 1      | 0         | 16446  | 48901   | 25000    | 75000     | 1.0066874   |
| 1      | 1         | 16296  | 47154   | 25000    | 75000     | 1.0273286   |
| 1      | 10        | 9940   | 6863    | 25000    | 75000     | 2.366244    |
| 1      | 11        | 1139   | 6145    | 25000    | 75000     | 0.62548053  |
| 1      | 2         | 17470  | 46879   | 25000    | 75000     | 1.0859531   |
| 1      | 3         | 13305  | 41163   | 25000    | 75000     | 0.97708744  |
| 1      | 4         | 9385   | 40848   | 25000    | 75000     | 0.7473175   |
| 1      | 5         | 14393  | 39246   | 25000    | 75000     | 1.0733235   |
| 1      | 6         | 11801  | 30952   | 25000    | 75000     | 1.1041096   |
| 1      | 7         | 5138   | 29608   | 25000    | 75000     | 0.59149253  |
| 1      | 8         | 8331   | 28687   | 25000    | 75000     | 0.9002107   |
| 1      | 9         | 1103   | 8893    | 25000    | 75000     | 0.44137654  |
+--------+-----------+--------+---------+----------+-----------+-------------+
```
Note: a neutral risk = 1.0.  If the risk is > 1.0, that implies the good rate is higher for customers who have that product. The converse is true for customers who have a loan type which has a risk below 1.0. <BR>
<BR>
This risk doesn’t take into account loans with a very small frequency and will be 0.0 when there are zero goods in the category. Smoothed risks remove these limitations. Calculate the <b>smoothed risk</b> for the loan types. <BR>
<BR>
The smoothing parameter controls the amount to which risks of small categories are pulled towards the neutral risk of 1.0. Experiment with the parameter to find the right level for your scenario. In the example below, smooth=50 is used.<BR>
<BR>
To write a table, we need to switch to the "risk" schema you created previously. Or you can specify the workspace and schema in the query. To set the risk as the default schema, use this command:<BR>
```
0: jdbc:drill:zk=local> use dfs.risk;
+-------+---------------------------------------+
|  ok   |                summary                |
+-------+---------------------------------------+
| true  | Default schema changed to [dfs.risk]  |
+-------+---------------------------------------+
```
```
create table dfs.risk.loanrisk as
select LOAN.*,TOTAL.numGood,TOTAL.numOther,
((cast(LOAN.goods as float)+50*LOAN.goods/(LOAN.goods+LOAN.others))/(50+LOAN.goods+LOAN.others)/TOTAL.numGood*(TOTAL.numGood+TOTAL.numOther)) `smoothedRisk` 
from (
select 1 as `dummy`,loanType,sum(goods) `goods`,sum(others) `others` from 
(
select loantype,case when good='1' then 1 else 0 end as `goods`,
           case when good='0' then 1 else 0 end as `others`
           from ( select loantype,good from dfs.`/Users/joeblue/RiskTables/loanType.json`)
) group by loanType ) `LOAN`
JOIN
( select 1 as `dummy`,sum(totalGood) `numGood`,sum(totalOther) `numOther` from (
 select case when good='1' then 1 else 0 end as `totalGood`,
           case when good='0' then 1 else 0 end as `totalOther`
           from ( select distinct acct,good from dfs.`/Users/joeblue/RiskTables/loanType.json`))
)`TOTAL`
ON LOAN.dummy=TOTAL.dummy;
+-----------+----------------------------+
| Fragment  | Number of records written  |
+-----------+----------------------------+
| 0_0       | 12                         |
+-----------+----------------------------+
```
Check out the risk table you just created:<BR>
```
0: jdbc:drill:zk=local> select * from dfs.risk.loanrisk
order by loanType;
+--------+-----------+--------+---------+----------+-----------+---------------+
| dummy  | loanType  | goods  | others  | numGood  | numOther  | smoothedRisk  |
+--------+-----------+--------+---------+----------+-----------+---------------+
| 1      | 0         | 16446  | 48901   | 25000    | 75000     | 1.0066516     |
| 1      | 1         | 16296  | 47154   | 25000    | 75000     | 1.0272756     |
| 1      | 10        | 9940   | 6863    | 25000    | 75000     | 2.366107      |
| 1      | 11        | 1139   | 6145    | 25000    | 75000     | 0.6250341     |
| 1      | 2         | 17470  | 46879   | 25000    | 75000     | 1.0859175     |
| 1      | 3         | 13305  | 41163   | 25000    | 75000     | 0.9770718     |
| 1      | 4         | 9385   | 40848   | 25000    | 75000     | 0.7472903     |
| 1      | 5         | 14393  | 39246   | 25000    | 75000     | 1.0732925     |
| 1      | 6         | 11801  | 30952   | 25000    | 75000     | 1.1040348     |
| 1      | 7         | 5138   | 29608   | 25000    | 75000     | 0.5914473     |
| 1      | 8         | 8331   | 28687   | 25000    | 75000     | 0.9001835     |
| 1      | 9         | 1103   | 8893    | 25000    | 75000     | 0.4411706     |
+--------+-----------+--------+---------+----------+-----------+---------------+
```
## Scoring customers for cumulative loan Risk
Use the risk table to determine if a customer with multiple loan products has a cumulative positive or negative risk for being a “good” customer. <BR>
<BR>
Join the smoothed risk table to the loanType json file. 
```
0: jdbc:drill:zk=local> select A.*,log(B.smoothedRisk) `logRisk` from
(select * from dfs.`/Users/joeblue/RiskTables/loanType.json`) `A`
JOIN
(select * from dfs.risk.loanrisk) `B`
ON A.loanType=B.loanType
limit 10;
+-------+-----------+-------+-----------------------+
| acct  | loanType  | good  |        logRisk        |
+-------+-----------+-------+-----------------------+
| 0     | 8         | 1     | -0.10515664881852095  |
| 0     | 10        | 1     | 0.8612459830459195    |
| 0     | 5         | 1     | 0.07073102092846928   |
| 0     | 4         | 1     | -0.29130153046917123  |
| 0     | 7         | 1     | -0.5251827056059107   |
| 0     | 2         | 1     | 0.08242522676358564   |
| 0     | 0         | 1     | 0.006629615393701478  |
| 0     | 6         | 1     | 0.09897145230065654   |
| 1     | 1         | 1     | 0.026910212663916817  |
| 1     | 4         | 1     | -0.29130153046917123  |
+-------+-----------+-------+-----------------------+
```
The sum of the ln(Risk) for each account's loans is the cumulative risk. Calculate and show the accounts with the highest and lowest cumulative loan type risks.<BR>
```
0: jdbc:drill:zk=local> select acct,sum(logRisk) `cumulativeRisk` from(
select A.*,log(B.smoothedRisk) `logRisk` from
(select * from dfs.`/Users/joeblue/RiskTables/loanType.json`) `A`
JOIN
(select * from dfs.risk.loanrisk) `B`
ON A.loanType=B.loanType
)
group by acct
order by sum(logRisk) desc
limit 10;
+--------+---------------------+
|  acct  |   cumulativeRisk    |
+--------+---------------------+
| 47320  | 1.1469135110962496  |
| 49894  | 1.1469135110962496  |
| 93248  | 1.1469135110962496  |
| 5544   | 1.1469135110962494  |
| 4957   | 1.1469135110962494  |
| 4649   | 1.1469135110962494  |
| 2685   | 1.1469135110962494  |
| 231    | 1.1469135110962494  |
| 1617   | 1.1469135110962494  |
| 873    | 1.1469135110962494  |
+--------+---------------------+
```
This query shows the accounts that have the most negative, cumulative log(risk). <BR>
```
0: jdbc:drill:zk=local> select acct,sum(logRisk) `cumulativeRisk` from(
select A.*,log(B.smoothedRisk) `logRisk` from
(select * from dfs.`/Users/joeblue/RiskTables/loanType.json`) `A`
JOIN
(select * from dfs.risk.loanrisk) `B`
ON A.loanType=B.loanType
)
group by acct
order by sum(logRisk) 
limit 10;
+--------+----------------------+
|  acct  |    cumulativeRisk    |
+--------+----------------------+
| 57528  | -2.183003376177795   |
| 36426  | -2.1830033761777945  |
| 7443   | -2.1440538638491216  |
| 37274  | -2.1440538638491216  |
| 15627  | -2.1440538638491216  |
| 1164   | -2.127507638312051   |
| 42795  | -2.1274883620781257  |
| 30383  | -2.1237732665789064  |
| 4878   | -2.1213224417941867  |
| 41379  | -2.1208587466844246  |
+--------+----------------------+
```
The risk table could be used to identify recommended products or the cumulative log(risk) could be used as an input to a predictive model. <BR>
<BR>
To make this lengthy query easier to work with, you could create a <b>Drill UDF</b> that contains the calculation and allows the user to specify the smooth parameter. For more info on UDF’s check out this link:<BR>
http://drill.apache.org/docs/adding-custom-functions-to-drill/ <BR>


