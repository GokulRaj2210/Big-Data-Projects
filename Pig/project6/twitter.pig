
Inp = LOAD '$G' USING PigStorage(',') AS (U_ID:long, F_ID:long);
groupfid= group Inp by F_ID; 
generatecount = FOREACH groupfid { generate group, COUNT(Inp);};
groupN = group generatecount by $1;
generateans = FOREACH groupN { generate group, COUNT($1);};
STORE generateans INTO '$O' USING PigStorage(',');
