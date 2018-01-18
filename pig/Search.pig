A = load 'job.txt' using TextLoader() as (job_desc:chararray);
register /home/hduser/pigudf.jar;
DEFINE TitleContains myudfs.Search();
B = foreach A generate TitleContains($0,'Data Engineer');
dump B;



 
