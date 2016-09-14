-- ================================================================================
-- ================================================================================
-- title : dihub demo
-- version : 0.9
-- date : 14-sept-2016
-- author 1 : mark oost, data scientist
-- author 2 : mireia alos palop, data scientist
-- ================================================================================
-- ================================================================================

-- TO DO: Replace <FILL_IN> with the appropriate SQL command.
-- TO DO: Replace <SCHEMA> your own schema.
-- quick look at the source data
select * from <SCHEMA> .gb_loans_id limit 100; 
select * from <SCHEMA> .gb_monthly_perform limit 100;


-- ================================================================================
-- ================================================================================
-- step 1 : naive bayes
-- ================================================================================
-- ================================================================================

-- data manipulation

drop table if exists <SCHEMA>.gb_loans_dev;
create table <SCHEMA>.gb_loans_dev distribute by hash(loan_seq_num) as 
select * from sample(on fm_demo.gb_loans_id  samplefraction('0.2'));
-- 20% sample

drop table if exists <SCHEMA>.gb_loans_test;
create table <SCHEMA>.gb_loans_test distribute by hash(loan_seq_num) as
select a.*  from fm_demo.gb_loans_id a left join <SCHEMA>.gb_loans_dev b on a.id=b.id where  
b.id is null;
-- 80% sample

select * from <SCHEMA>.gb_loans_dev limit 100; 
select * from <SCHEMA>.gb_loans_test limit 100;

------naives bayes - construction model ------------

drop table if exists <SCHEMA>.naivesbayes;
create table <SCHEMA>.naivesbayes (partition key(class)) as
select * from naivebayesreduce(
              on(
                     select * from naivebayesmap(
                           on  <SCHEMA>.gb_loans_dev 
                           response('foreclose_flg')
                           numericinputs('credit_score', 'orig_dti', 'orig_ltv', 'num_borrowers')
			   categoricalinputs('property_type_cd')
                           )
              )
partition by class);
-- construct nb model

select * from <SCHEMA> .naivesbayes;


------ naives bayes - prediction model -------------


drop table if exists   <SCHEMA>.naivesbayes_pred;
create table  <SCHEMA>.naivesbayes_pred distribute by hash(id) as
select a.prediction, a."loglik_1", a."loglik_0",b.* from (
       select * from naivebayespredict(
              on <SCHEMA>.gb_loans_test
              model('<SCHEMA>.naivesbayes')
              idcol('id')
              numericinputs('credit_score', 'orig_dti', 'orig_ltv', 'num_borrowers')
	      categoricalinputs('property_type_cd')
              )
           ) a
inner join <SCHEMA>.gb_loans_test b on a.id=b.id;
-- predict back to the 80% data

select * from <SCHEMA>.naivesbayes_pred  limit 100; 
-- truth is downup


------ confusion matrix -------------------

drop table if exists <SCHEMA>.naivesnayes_conf_matrix;
select * from confusionmatrix(
on <SCHEMA>.naivesbayes_pred partition by 1
expectcolumn('foreclose_flg_c')
predictcolumn('prediction')
outputtable('<SCHEMA> .naivesnayes_conf_matrix')
);


select * from <SCHEMA> .naivesnayes_conf_matrix ;

------ cleaning -------------------

drop table if exists <SCHEMA>.gb_loans_dev;
drop table if exists <SCHEMA>.gb_loans_test;
drop table if exists <SCHEMA>.naivesbayes;
drop table if exists <SCHEMA>.naivesbayes_pred;
drop table if exists <SCHEMA>.naivesnayes_conf_matrix;

-- ================================================================================
-- ================================================================================
-- step 2 : glm
-- ================================================================================
-- ================================================================================


drop table if exists <SCHEMA>.gb_loans_dev;
create table <SCHEMA>.gb_loans_dev distribute by hash(loan_seq_num) as 
select * from sample(on fm_demo.gb_loans_id  samplefraction('0.2'));
-- 20% sample

drop table if exists <SCHEMA>.gb_loans_test;
create table <SCHEMA>.gb_loans_test distribute by hash(loan_seq_num) as
select a.*  from fm_demo.gb_loans_id a left join <SCHEMA>.gb_loans_dev b on a.id=b.id where  
b.id is null;
-- 80% sample

select * from <SCHEMA>.gb_loans_dev limit 100; 
select * from <SCHEMA>.gb_loans_test limit 100;

------ glm - construction model ------------

drop table if exists <SCHEMA> .gb_loans2006_glm;
select * from glm (
		on (select 1)
		partition by 1
		inputtable('<SCHEMA>.gb_loans_dev')
		outputtable('<SCHEMA>.gb_loans2006_glm')
		columnnames('foreclose_flg_nr', 'credit_score', 'orig_dti', 'orig_ltv', 'num_borrowers','property_type_cd')
		categoricalcolumns('property_type_cd')
		family('logistic')
		link('logit')
		maxiternum('10')
);

------ glm - prediction model -------------

 drop table if exists <SCHEMA>.score_glm;
create table <SCHEMA>.score_glm distribute by hash(id) as
select * from glmpredict (
		on <SCHEMA> .gb_loans_test
		modeltable ('<SCHEMA>.gb_loans2006_glm')
		accumulate ('id', 'foreclose_flg')
		family ('logistic')
		link ('logit')
);

select * from <SCHEMA>.score_glm;

------ cleaning -------------------

drop table if exists <SCHEMA>.gb_loans_dev;
drop table if exists <SCHEMA>.gb_loans_test;
drop table if exists <SCHEMA>.gb_loans2006_glm;
drop table if exists <SCHEMA>.score_glm;

-- ================================================================================
-- ================================================================================
-- step 3 : kmeans
-- ================================================================================
-- ================================================================================

-- data manipulation
drop table if exists <SCHEMA>.gb_loans_kmeans_input;
create analytic table <SCHEMA>.gb_loans_kmeans_input (
    loan_seq_num varchar(12) 
    ,credit_score numeric
    ,orig_dti numeric
    ,orig_ltv numeric
    ,first_pay_dt numeric
    ,firstime_buyer_flg numeric 
    ,mature_dt numeric
    ,mortgage_insr_pct numeric
    ,num_units numeric
    ,orig_cltv numeric
    ,orig_upb numeric
    ,orig_interest_rt numeric
    ,orig_loan_term numeric
    ,num_borrowers numeric 
      )
distribute by hash (loan_seq_num)
as 
select     loan_seq_num 
    ,credit_score
    ,orig_dti 
    ,orig_ltv 
    ,first_pay_dt 
    ,firstime_buyer_flg 
    ,mature_dt 
    ,mortgage_insr_pct 
    ,num_units 
    ,orig_cltv 
    ,orig_upb 
    ,orig_interest_rt 
    ,orig_loan_term 
    ,num_borrowers
from fm_demo.gb_loans;

analyze <SCHEMA>.gb_loans_kmeans_input;

------ kmeans - construction clusters ------------

drop table if exists <SCHEMA> .gb_loans_kmeans_output;
select * from kmeans(
		on (select 1)
		partition by 1
		inputtable('<SCHEMA>.gb_loans_kmeans_input')
		outputtable('<SCHEMA>.gb_loans_kmeans_output')
		numberk(2)
);
select * from <SCHEMA>.gb_loans_kmeans_output;

------ kmeans - prediction clusters ------------

drop table if exists <SCHEMA>.gb_loans_kmeansplot;
create table <SCHEMA>.gb_loans_kmeansplot distribute by hash (loan_seq_num) as
select * from kmeansplot(
		on <SCHEMA>.gb_loans_kmeans_input partition by any
		on <SCHEMA>.gb_loans_kmeans_output dimension
		centroidstable('<SCHEMA>.gb_loans_kmeans_output')
);

select * from <SCHEMA> .gb_loans_kmeansplot;

------ cleaning -------------------

drop table if exists <SCHEMA>.gb_loans_kmeans_input;
drop table if exists <SCHEMA>.gb_loans_kmeans_output;
drop table if exists <SCHEMA>.gb_loans_kmeansplot;

-- ================================================================================
-- ================================================================================
-- Step 4 : HMM Model
-- ================================================================================
-- ================================================================================

--- This nested sql creates the main input table for a HMM model
drop table if exists <SCHEMA>.gb_monthly_perform_hmm_input;
create table <SCHEMA>.gb_monthly_perform_hmm_input distribute by hash (loan_seq_num) as
select *
	  --- we create the state column
	, case
	  when observation = 'defaulted' then 'defaulted'
	  when curr_delinqency_cd != 0 then 'delinquent'
	  else 'performing'
	  end as state
from ( --- we create the observation column 
	select *
	, case
	  when curr_interest_rt < lag_int_rt and lag_int_rt != 0 then 'interest_down'
	  when curr_interest_rt > lag_int_rt and lag_int_rt != 0 then 'interest_up'
	  when curr_act_upb = 0 and lag_upb = 0 then 'ignore'
	  when lag_upb = 0 and curr_delinqency_cd =0 and zero_bal_cd = '0' then 'start'
	  when curr_act_upb < lag_upb and curr_delinqency_cd =0 and zero_bal_cd = '0' then 'paid'
	  when curr_act_upb = lag_upb and curr_act_upb != 0 then  'missed'
	  when curr_act_upb < lag_upb and curr_delinqency_cd !=0 and foreclose_flg ='0' then 'late'
	  when foreclose_flg = '1' then 'defaulted'
	  when curr_act_upb = 0 and zero_bal_cd = '1' then 'full_payoff'
	  else 'unknown'
	  end as observation
from ( --- selecting important columns and adding the lag for some
	select loan_seq_num
	, loan_age
	, curr_act_upb
	, curr_delinqency_cd
	, lag(loan_age,1,0) over (partition by loan_seq_num order by loan_age asc) as "lag_age"
	, lag(curr_act_upb,1,0) over (partition by loan_seq_num order by loan_age asc) as "lag_upb"
	, zero_bal_cd
	, curr_interest_rt
	, lag(curr_interest_rt,1,0) over (partition by loan_seq_num order by loan_age asc) as "lag_int_rt"
	, modify_flg
	, foreclose_flg
from "fm_demo"."gb_monthly_perform"
--- here we select loan_seq_num with more than 10 payments
where  loan_seq_num in (select loan_seq_num
from fm_demo.gb_monthly_perform
group by loan_seq_num
having count(*) > 10
limit 1000)
) as a
) as b
-- remove observations that are not important
where observation != 'ignore' and observation != 'unknown'
order by loan_age asc
;

--- inspect the data
select state, observation, count(*)
from <SCHEMA>.gb_monthly_perform_hmm_input
group by state, observation 
;

--- create a hmm model with three outputs:
--- table 1: hmm_pi_loans -> initial state probability table
--- table 2: hmm_trans states -> State-Transition Probability Table
--- table 3: hmm_trans_states_ops -> Emission probability table
drop table if exists <SCHEMA>.hmm_pi_loans, <SCHEMA>.hmm_trans_states, <SCHEMA>.hmm_trans_states_obs;
SELECT * FROM HMMSupervisedLearner (
ON <SCHEMA> .gb_monthly_perform_hmm_input AS "vertices"
PARTITION BY loan_seq_num
ORDER BY loan_seq_num, loan_age ASC
SequenceKey ('loan_seq_num')
ObservedKey ('observation')
StateKey ('state')
OutputTables ('<SCHEMA>.hmm_pi_loans', '<SCHEMA>.hmm_trans_states', '<SCHEMA>.hmm_trans_states_obs')
);

------ Cleaning -------------------

drop table if exists <SCHEMA>.gb_monthly_perform_hmm_input;
drop table if exists <SCHEMA>.hmm_pi_loans;
drop table if exists <SCHEMA>.hmm_trans_states; 
drop table if exists <SCHEMA>.hmm_trans_states_obs;
