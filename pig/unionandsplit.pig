book1 = load '/home/hduser/book1.txt' using PigStorage() as (lines:chararray);
book2 = load '/home/hduser/book2.txt' using PigStorage() as (lines:chararray);

bookcombined = union book1, book2;

--dump bookcombined;
(this is a sentence one)
(this is a sentence four)
(this is a sentence two)
(this is a sentence five)
(this is a sentence three)
(this is a sentence six)
(this is a sentence eight)
(this is a sentence seven)


split bookcombined into book3 if SUBSTRING (lines, 5,7) == 'is', book4 if SUBSTRING (lines, 19, 24) == 'three', book5 if SUBSTRING(lines, 0, 4 ) == 'this';


--dump book3;
--dump book4;
--dump book5;


--log files
--/usr/local/hadoop/logs/mapred-hduser-historyserver-ubuntu.log 

log1 = load '/home/hduser/mapred-hduser-historyserver-ubuntu.log' using PigStorage() as (lines:chararray);


split log1 into log2 if SUBSTRING (lines, 24, 28) == 'INFO', log3 if SUBSTRING (lines, 24, 29) == 'ERROR', log4 if SUBSTRING (lines, 24, 28) == 'WARN';

groupoflog2 = group log2 ALL;
groupoflog3 = group log3 ALL;
groupoflog4 = group log4 ALL;

countoflog2 = foreach groupoflog2 generate COUNT(log2);
countoflog3 = foreach groupoflog3 generate COUNT(log3);
countoflog4 = foreach groupoflog4 generate COUNT(log4);

dump countoflog2;
dump countoflog3;
dump countoflog4;

--mapper
infogroup = group log2 by SUBSTRING(lines,20,23);
dump infogroup;

--reducer
infocount = foreach infogroup generate group, COUNT(log2);

infoorder = order infocount by $1;
dump infoorder;

errorgroup = group log3 by SUBSTRING(lines,20,23);
errorcount = foreach errorgroup generate group, COUNT(log3);
errororder = order errorcount by $1;
dump errororder;


--We have 3362 tuples for INFO messages
--we have 39 tuples for error msgs
--we have 35 tuples for WARN


errordtgroup = group log3 by SUBSTRING(lines,0,10);
errordtcount = foreach errordtgroup generate group, COUNT(log3);
dump errordtcount;





