InputGraph = LOAD '$G' USING PigStorage(',') AS (a: int,b: int);
A =  GROUP InputGraph BY a;
ACount = FOREACH A GENERATE group, COUNT(InputGraph);
MidGraph = FOREACH ACount GENERATE $1 as C, $0 as D;
B = GROUP MidGraph by C;
BCount = FOREACH B GENERATE group,COUNT(MidGraph);

STORE BCount INTO '$O' USING PigStorage (','); 
