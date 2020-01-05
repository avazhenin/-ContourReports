select /*+parallel(32)*/ 
         t1.Dt
       , t1.Subs_Id n
       , sh.trpl_id tarif_n
       , t2.t2_region_name lac
       , t1.EVENT_DATE strt
       , 26 code
       , cl.account
       , p.msisdn mob_num
       , s.ACTIVATION_DATE begin_work
       , get_begin_work_com(t1.subs_id,1) begin_work_com
       , decode(nvl(sod.field_value,'kz'),'ru',1,'kz',3,2) lang
       , sh.st_id prepaid_platform
       , con.dlr_id r_app
       , u.usi imsi
       , t2.summ amount
       , ch.clnt_id
       , t3.balance_$
       , sh.zone_id
       , nvl(t2.lac_a,'N\A') full_lac
       , t2.call_date zr_dt
from (
select "DT","SUBS_ID","EVENT_DATE","PAID_SESSION_DATE","PAYMENT_DATE","CHARGE_DATE" from (
select 
               to_date('19.02.2016','dd.mm.yyyy') dt,
               subs_id,
               max(event_date) event_date,
               max(nvl(paid_session_date,to_date('01.01.1988','dd.mm.yyyy'))) paid_session_date,
               max(nvl(payment_date,to_date('01.01.1988','dd.mm.yyyy'))) payment_date,
               max(nvl(charge_date,to_date('01.01.1988','dd.mm.yyyy'))) charge_date
          from (
        select ct.subs_id,
               ct.call_date event_date,
               ct.call_date paid_session_date,
               to_date('01.01.1988','dd.mm.yyyy') payment_date,
               to_date('01.01.1988','dd.mm.yyyy') charge_date
          from contour.contour_traffic ct
         where ct.call_date    >= trunc(to_date('19.02.2016','dd.mm.yyyy'))-90
           and ct.call_date    <  trunc(to_date('19.02.2016','dd.mm.yyyy'))+1
           and ct.sum_price_$  > 0
     union all
     /* платежи */
select pay.subs_id, max(event_date) event_date, max(paid_session_date) paid_session_date, max(payment_date) payment_date, max(charge_date) charge_date
 from (
        select sh.subs_id,
               pd.cre_date event_date,
               to_date('01.01.1988','dd.mm.yyyy') paid_session_date,
               pd.cre_date payment_date,
               to_date('01.01.1988','dd.mm.yyyy') charge_date
        from payment 		p 
        join pay_doc 		pd on pd.pdoc_id=p.pdoc_id 
         and pd.summ_$>=150 
         and pd.del_user_id is null
        join subs_history 	sh on sh.clnt_id=p.clnt_id 
         and pd.cre_date >= sh.stime 
         and pd.cre_date < 	sh.etime 
         and pd.cre_date >= trunc(to_date('19.02.2016','dd.mm.yyyy'))-90
         and pd.cre_date <  trunc(to_date('19.02.2016','dd.mm.yyyy'))+1         
         and pd.pt_id = 1 /* Если это банковская оплата, то исключаем СПИСАНИЕ ДЗ / КЗ */
         and sh.stat_id not in (0,3)
        join pay_list 		pl on pl.plst_id=pd.plst_id 
        and pl.bank_id not in (34,74) /* исключаем СПИСАНИЕ ДЗ / КЗ */
        union 
        select sh.subs_id,
               pd.cre_date event_date,
               to_date('01.01.1988','dd.mm.yyyy') paid_session_date,
               pd.cre_date payment_date,
               to_date('01.01.1988','dd.mm.yyyy') charge_date
        from payment 		p 
        join pay_doc 		pd on pd.pdoc_id=p.pdoc_id 
         and pd.summ_$ 		>= 150 
         and pd.del_user_id is null
         and pd.cre_date 	>= trunc(to_date('19.02.2016','dd.mm.yyyy'))-90
         and pd.cre_date 	<  trunc(to_date('19.02.2016','dd.mm.yyyy'))+1         
        join subs_history 	sh on sh.clnt_id=p.clnt_id 
         and pd.cre_date 	>= sh.stime 
         and pd.cre_date 	< sh.etime  
         and (pd.pt_id > 1 or pd.pt_id=-1) /* учитываем все кроме банковской оплаты + перенос баланса */
         and sh.stat_id not in (0,3) /* не учитываем платежи на подготовленных и закрытых абонентах */
         )pay group by pay.subs_id
     union all
     /* начисления */
        select ch.subs_id,
               ch.charge_date event_date,
               to_date('01.01.1988','dd.mm.yyyy') paid_session_date,
               to_date('01.01.1988','dd.mm.yyyy') payment_date,
               ch.charge_date charge_date
          from charge 	ch
     left join service 	serv on ch.serv_id=serv.serv_id
         where ch.charge_date >= trunc(to_date('19.02.2016','dd.mm.yyyy'))-90 
           and ch.charge_date <  trunc(to_date('19.02.2016','dd.mm.yyyy'))+1
           and ch.del_date is null
           and ch.chtype_id = 2 /* Only subs charges  */                      
           and ch.summ_$ > 0)
      group by subs_id)
      where (paid_session_date <> to_date('01.01.1988','dd.mm.yyyy') or
             payment_date      <> to_date('01.01.1988','dd.mm.yyyy') or
             charge_date       <> to_date('01.01.1988','dd.mm.yyyy'))
) t1
left join (select max(T2_Region_Name)T2_Region_Name, Subs_Id, max(Lac_a)Lac_a, max(Call_Date)call_date, sum(summ) summ from(
select z.T2_Region_Name, t.Subs_Id, T2.Lac_a, T3.Call_Date, t.summ
          from (select Subs_Id,
                       sum(Sum_Price_$) summ,
                       max(Ctraffic_Id) Ctraffic_Id,
                       max(case when Sum_Price_Wo_Dis != 0 then Ctraffic_Id end) Lcd_Ctraffic_Id
                  from Contour_Traffic
                 where Call_Date >= trunc(to_date('19.02.2016','dd.mm.yyyy'))-90 
                   and Call_Date <  trunc(to_date('19.02.2016','dd.mm.yyyy'))+1
                 group by Subs_Id) t
          join Contour_Traffic T2 on t.Ctraffic_Id = T2.Ctraffic_Id
          join Contour_Traffic T3 on t.Lcd_Ctraffic_Id = T3.Ctraffic_Id
          join T2_Zone2region z   on T2.Zone_Id = z.Zone_Id
         union all
        select null, ch.subs_id,null,null, sum(ch.summ_$) summ 
          from charge ch 
         where ch.del_date is null
           and ch.charge_date >= trunc(to_date('19.02.2016','dd.mm.yyyy'))-90 
           and ch.charge_date <  trunc(to_date('19.02.2016','dd.mm.yyyy'))+1        
      group by  ch.subs_id) group by Subs_Id) t2 
      on t1.subs_id=t2.subs_id   
 join Subs_History                    Sh  on t1.Subs_Id = Sh.Subs_Id
 --left join t2_subs_begin_work_com     bwc on t1.SUBS_ID = bwc.c_subs_id
 join client_history                  ch  on sh.clnt_id = ch.clnt_id     
 join contract                        con on ch.clnt_id = con.clnt_id   
 left join subs_usi_history           suh on sh.subs_id=suh.subs_id
  and t1.dt+1-1/86400 >= suh.Stime
  and t1.dt+1-1/86400 <  suh.Etime    
 left join usi                        u   on suh.usi_id=u.usi_id
  join (select least(nvl(s.activation_date,to_date('31.12.2999','dd.mm.yyyy')),sh.stime) activation_date, s.subs_id
         from subscriber s
         join (select min(stime) stime, subs_id from subs_history group by subs_id) sh on s.subs_id=sh.subs_id ) s on t1.subs_id=s.subs_id
 join (select Cb.Clnt_Id, sum(Cb.Balance_$) Balance_$
         from Client_Balance Cb
         join Smaster.Balance_Dict Bd
           on Cb.Balance_Id = Bd.Balance_Id
          and Bd.Bal_Type_Id not in (2, 5)
     group by Cb.Clnt_Id)             t3  on ch.clnt_id=t3.clnt_id
 left join subs_opt_data              sod on t1.SUBS_ID=sod.subs_id 
  and sod.subs_fld_id=32
  and t1.dt+1-1/86400 >= sod.stime 
  and t1.dt+1-1/86400 <= sod.etime
 join client                          cl  on sh.clnt_id=cl.clnt_id
 left join phone                      p   on sh.phone_id=p.phone_id
 where t1.dt+1-1/86400 >= Sh.Stime
   and t1.dt+1-1/86400 <  Sh.Etime
   and t1.dt+1-1/86400 >= ch.Stime
   and t1.dt+1-1/86400 <  ch.Etime 
   and t1.dt+1-1/86400 >= con.Stime
   and t1.dt+1-1/86400 <  con.Etime
   