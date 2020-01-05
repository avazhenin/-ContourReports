/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ContourReports;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author vazhenin
 */
public class Tele2Reports {

    String prodURL;
    String prodUser;
    String prodPWD;

    String stbURL;
    String stbUser;
    String stbPWD;

    String log4jPath;
    ParseXMLUtilities xml;
    static Logger log = Logger.getLogger(Tele2Reports.class);
    SimpleDateFormat sdf = new SimpleDateFormat("dd.MM.yyyy");
    long startTime;

    public Tele2Reports(String paramFilePath) {
        xml = new ParseXMLUtilities(paramFilePath);
        xml.initiate();
        initialize();
    }

    void T2_Active_Subscribers(java.util.Date d, boolean dbms_out) {
        PreparedStatement stbStatement = null;
        PreparedStatement prodStatement = null;

        String SQL = "select /*+parallel(32)*/ \n"
                + "         t1.Dt\n"
                + "       , t1.Subs_Id n\n"
                + "       , sh.trpl_id tarif_n\n"
                + "       , t2.t2_region_name lac\n"
                + "       , t1.EVENT_DATE strt\n"
                + "       , 26 code\n"
                + "       , cl.account\n"
                + "       , p.msisdn mob_num\n"
                + "       , s.ACTIVATION_DATE begin_work\n"
                + "       , greatest(s.activation_date,nvl(bwc.event_date,to_date('01.01.1888','dd.mm.yyyy'))) begin_work_com\n"
                + "       , decode(nvl(sod.field_value,'kz'),'ru',1,'kz',3,2) lang\n"
                + "       , sh.st_id prepaid_platform\n"
                + "       , con.dlr_id r_app\n"
                + "       , u.usi imsi\n"
                + "       , t2.summ amount\n"
                + "       , ch.clnt_id\n"
                + "       , t3.balance_$\n"
                + "       , sh.zone_id\n"
                + "       , nvl(t2.lac_a,'N\\A') full_lac\n"
                + "       , t2.call_date zr_dt\n"
                + "from (\n"
                + "select \"DT\",\"SUBS_ID\",\"EVENT_DATE\",\"PAID_SESSION_DATE\",\"PAYMENT_DATE\",\"CHARGE_DATE\" from (\n"
                + "select \n"
                + "               %dt% dt,\n"
                + "               subs_id,\n"
                + "               max(event_date) event_date,\n"
                + "               max(nvl(paid_session_date,to_date('01.01.1988','dd.mm.yyyy'))) paid_session_date,\n"
                + "               max(nvl(payment_date,to_date('01.01.1988','dd.mm.yyyy'))) payment_date,\n"
                + "               max(nvl(charge_date,to_date('01.01.1988','dd.mm.yyyy'))) charge_date\n"
                + "          from (\n"
                + "        select ct.subs_id,\n"
                + "               ct.call_date event_date,\n"
                + "               ct.call_date paid_session_date,\n"
                + "               to_date('01.01.1988','dd.mm.yyyy') payment_date,\n"
                + "               to_date('01.01.1988','dd.mm.yyyy') charge_date\n"
                + "          from contour.contour_traffic ct\n"
                + "         where ct.call_date    >= trunc(%dt%)-90\n"
                + "           and ct.call_date    <  trunc(%dt%)+1\n"
                + "           and ct.sum_price_$  > 0\n"
                + "     union all\n"
                + "     /* платежи */\n"
                + "select pay.subs_id, max(event_date) event_date, max(paid_session_date) paid_session_date, max(payment_date) payment_date, max(charge_date) charge_date\n"
                + " from (\n"
                + "        select sh.subs_id,\n"
                + "               pd.cre_date event_date,\n"
                + "               to_date('01.01.1988','dd.mm.yyyy') paid_session_date,\n"
                + "               pd.cre_date payment_date,\n"
                + "               to_date('01.01.1988','dd.mm.yyyy') charge_date\n"
                + "        from payment 		p \n"
                + "        join pay_doc 		pd on pd.pdoc_id=p.pdoc_id \n"
                + "         and pd.summ_$>=150 \n"
                + "         and pd.del_user_id is null\n"
                + "        join subs_history 	sh on sh.clnt_id=p.clnt_id \n"
                + "         and pd.cre_date >= sh.stime \n"
                + "         and pd.cre_date < 	sh.etime \n"
                + "         and pd.cre_date >= trunc(%dt%)-90\n"
                + "         and pd.cre_date <  trunc(%dt%)+1         \n"
                + "         and pd.pt_id = 1 /* Если это банковская оплата, то исключаем СПИСАНИЕ ДЗ / КЗ */\n"
                + "         and sh.stat_id not in (0,3)\n"
                + "        join pay_list 		pl on pl.plst_id=pd.plst_id \n"
                + "        and pl.bank_id not in (34,74) /* исключаем СПИСАНИЕ ДЗ / КЗ */\n"
                + "        union \n"
                + "        select sh.subs_id,\n"
                + "               pd.cre_date event_date,\n"
                + "               to_date('01.01.1988','dd.mm.yyyy') paid_session_date,\n"
                + "               pd.cre_date payment_date,\n"
                + "               to_date('01.01.1988','dd.mm.yyyy') charge_date\n"
                + "        from payment 		p \n"
                + "        join pay_doc 		pd on pd.pdoc_id=p.pdoc_id \n"
                + "         and pd.summ_$ 		>= 150 \n"
                + "         and pd.del_user_id is null\n"
                + "         and pd.cre_date 	>= trunc(%dt%)-90\n"
                + "         and pd.cre_date 	<  trunc(%dt%)+1         \n"
                + "        join subs_history 	sh on sh.clnt_id=p.clnt_id \n"
                + "         and pd.cre_date 	>= sh.stime \n"
                + "         and pd.cre_date 	< sh.etime  \n"
                + "         and (pd.pt_id > 1 or pd.pt_id=-1) /* учитываем все кроме банковской оплаты + перенос баланса */\n"
                + "         and sh.stat_id not in (0,3) /* не учитываем платежи на подготовленных и закрытых абонентах */\n"
                + "         )pay group by pay.subs_id\n"
                + "     union all\n"
                + "     /* начисления */\n"
                + "        select ch.subs_id,\n"
                + "               ch.charge_date event_date,\n"
                + "               to_date('01.01.1988','dd.mm.yyyy') paid_session_date,\n"
                + "               to_date('01.01.1988','dd.mm.yyyy') payment_date,\n"
                + "               ch.charge_date charge_date\n"
                + "          from charge 	ch\n"
                + "          join itog_balance ib on ch.ob_id=ib.ob_id\n"
                + "     left join service 	serv on ch.serv_id=serv.serv_id\n"
                + "         where ch.cre_date >= trunc(%dt%)-90 \n"
                + "           and ch.cre_date <  trunc(%dt%)+1\n"
                + "           and ib.ob_edate    >= trunc(%dt%)-90\n"
                + "           and ib.ob_edate    <  trunc(%dt%)+1\n"
                + "           and ch.del_date is null\n"
                + "           and ch.chtype_id = 2 /* Only subs charges  */\n"
                + "           and ch.summ_$ > 0)\n"
                + "      group by subs_id)\n"
                + "      where (paid_session_date <> to_date('01.01.1988','dd.mm.yyyy') or\n"
                + "             payment_date      <> to_date('01.01.1988','dd.mm.yyyy') or\n"
                + "             charge_date       <> to_date('01.01.1988','dd.mm.yyyy'))\n"
                + ") t1\n"
                + "left join (select max(T2_Region_Name)T2_Region_Name, Subs_Id, max(Lac_a)Lac_a, max(Call_Date)call_date, sum(summ) summ from(\n"
                + "select z.T2_Region_Name, t.Subs_Id, T2.Lac_a, T3.Call_Date, t.summ\n"
                + "          from (select Subs_Id,\n"
                + "                       sum(Sum_Price_$) summ,\n"
                + "                       max(Ctraffic_Id) Ctraffic_Id,\n"
                + "                       max(case when Sum_Price_Wo_Dis != 0 then Ctraffic_Id end) Lcd_Ctraffic_Id\n"
                + "                  from Contour_Traffic\n"
                + "                 where Call_Date >= trunc(%dt%)-90 \n"
                + "                   and Call_Date <  trunc(%dt%)+1\n"
                + "                 group by Subs_Id) t\n"
                + "          join Contour_Traffic T2 on t.Ctraffic_Id = T2.Ctraffic_Id\n"
                + "          join Contour_Traffic T3 on t.Lcd_Ctraffic_Id = T3.Ctraffic_Id\n"
                + "          join T2_Zone2region z   on T2.Zone_Id = z.Zone_Id\n"
                + "         union all\n"
                + "        select null, ch.subs_id,null,null, sum(ch.summ_$) summ \n"
                + "          from charge ch \n"
                + "         where ch.del_date is null\n"
                + "           and ch.cre_date >= trunc(%dt%)-90 \n"
                + "           and ch.cre_date <  trunc(%dt%)+1  \n"
                + "      group by  ch.subs_id) group by Subs_Id) t2 \n"
                + "      on t1.subs_id=t2.subs_id   \n"
                + " join Subs_History                    Sh  on t1.Subs_Id = Sh.Subs_Id\n"
                // + " left join t2_subs_begin_work_com     bwc on t1.SUBS_ID = bwc.c_subs_id\n"
                + " join client_history                  ch  on sh.clnt_id = ch.clnt_id     \n"
                + " join contract                        con on ch.clnt_id = con.clnt_id   \n"
                + " left join subs_usi_history           suh on sh.subs_id=suh.subs_id\n"
                + "  and t1.dt+1-1/86400 >= suh.Stime\n"
                + "  and t1.dt+1-1/86400 <  suh.Etime    \n"
                + " left join usi                        u   on suh.usi_id=u.usi_id\n"
                + "  join (select greatest(nvl(s.activation_date,to_date('31.12.1899','dd.mm.yyyy')),sh.stime) activation_date, s.subs_id\n"
                + "         from subscriber s\n"
                + "         join (select min(stime) stime, subs_id from subs_history group by subs_id) sh on s.subs_id=sh.subs_id ) s on t1.subs_id=s.subs_id\n"
                + " join (select Cb.Clnt_Id, sum(Cb.Balance_$) Balance_$\n"
                + "         from Client_Balance Cb\n"
                + "         join Smaster.Balance_Dict Bd\n"
                + "           on Cb.Balance_Id = Bd.Balance_Id\n"
                + "          and Bd.Bal_Type_Id not in (2, 5)\n"
                + "     group by Cb.Clnt_Id)             t3  on ch.clnt_id=t3.clnt_id\n"
                + " left join subs_opt_data              sod on t1.SUBS_ID=sod.subs_id \n"
                + "  and sod.subs_fld_id=32\n"
                + "  and t1.dt+1-1/86400 >= sod.stime \n"
                + "  and t1.dt+1-1/86400 <= sod.etime\n"
                + " join client                          cl  on sh.clnt_id=cl.clnt_id\n"
                + " left join phone                      p   on sh.phone_id=p.phone_id\n"
                + "  left join (select nvl(min(Event_Date),to_date('01.01.1888','dd.mm.yyyy')) Event_Date, Subs_Id\n"
                + "               from (select min(Pd.Pdoc_Date) Event_Date, p.Subs_Id\n"
                + "                       from Payment p\n"
                + "                       join Pay_Doc Pd\n"
                + "                         on p.Pdoc_Id = Pd.Pdoc_Id\n"
                + "                      group by p.Subs_Id\n"
                + "                     union all\n"
                + "                     select min(Call_Date) Event_Date, Subs_Id\n"
                + "                       from Contour_Traffic\n"
                + "                      where Sum_Price_$ > 0\n"
                + "                      group by Subs_Id\n"
                + "                     union all\n"
                + "                     select min(Ch.Charge_Date) Event_Date, Ch.Subs_Id\n"
                + "                       from Charge Ch\n"
                + "                      where Ch.Chtype_Id=2\n"
                + "                        and Ch.Del_Date is null\n"
                + "                        and Ch.Summ_$ > 0\n"
                + "                      group by Ch.Subs_Id)\n"
                + "              group by Subs_Id) bwc on t1.subs_id=bwc.subs_id"
                + " where t1.dt+1-1/86400 >= Sh.Stime\n"
                + "   and t1.dt+1-1/86400 <  Sh.Etime\n"
                + "   and t1.dt+1-1/86400 >= ch.Stime\n"
                + "   and t1.dt+1-1/86400 <  ch.Etime \n"
                + "   and t1.dt+1-1/86400 >= con.Stime\n"
                + "   and t1.dt+1-1/86400 <  con.Etime\n"
                + "   and sh.stat_id not in (3) /* закрытых абонентов не учитываем */\n"
                + "   and ch.ct_id not in (4,5,0) /* не учитываем служебные номера */\n"
                + "   and bwc.Event_Date < t1.dt+1 /* BEGIN_WORK_COM не может быть больше даты выборки */ \n" //                + "   and t1.subs_id=5915762\n"
                ;

        Connection prod = establishConn(prodURL, prodUser, prodPWD);
        Connection stb = establishConn(stbURL, stbUser, stbPWD);
        if (prod == null || stb == null) {
            log.fatal(prod);
            log.fatal(stb);
            System.exit(1);
        }
        try {
            /* get SQL */
//            insertSQL = read_sql(insertSQL);
            SQL = SQL.replace("%dt%", "to_date('" + new SimpleDateFormat("dd.MM.yyyy").format(d).toString() + "','dd.mm.yyyy')");
//            log.info(insertSQL);
            if (dbms_out) {
                log.info(SQL);
                return;
            }

            /* Turn off autocommit on prod database */
            prod.setAutoCommit(false);

            /* delete data for specified date */
            String deleteSQL = "delete Contour.T2_Active_Subscribers t where t.dt=?";
            prodStatement = prod.prepareStatement(deleteSQL);
            prodStatement.setTimestamp(1, new Timestamp(d.getTime()));
            log.info("Start process : Delete Contour.T2_Active_Subscribers where DT='" + sdf.format(d).toString() + "'");
            setTimer();
            prodStatement.executeUpdate();
            log.info("End process : Delete Contour.T2_Active_Subscribers where DT='" + sdf.format(d).toString() + getElapsedTime());


            /* prepare Query statement for standby  */
            stbStatement = stb.prepareStatement(SQL);//, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stbStatement.setFetchSize(10000);
            /* prepare Insert statement for Prod  */
            String DML = "insert into Contour.T2_Active_Subscribers(DT,\n"
                    + "N,\n"
                    + "TARIF_N,\n"
                    + "LAC,\n"
                    + "STRT,\n"
                    + "CODE,\n"
                    + "ACCOUNT,\n"
                    + "MOB_NUM,\n"
                    + "BEGIN_WORK,\n"
                    + "BEGIN_WORK_COM,\n"
                    + "LANG,\n"
                    + "PREPAID_PLATFORM,\n"
                    + "R_APP,\n"
                    + "IMSI,\n"
                    + "AMOUNT,\n"
                    + "CLNT_ID,\n"
                    + "BALANCE_$,\n"
                    + "ZONE_ID,\n"
                    + "FULL_LAC,\n"
                    + "ZR_DT) values(" + getIntoBinds(stbStatement.getMetaData()) + ")";
            prodStatement = prod.prepareStatement(DML);

            log.info("Start executing Query");
            setTimer();
            ResultSet rs = stbStatement.executeQuery(); // execute query
            log.info("Stop executing Query" + getElapsedTime());

            /* Fetch rows */
            log.info("Start inserting records");
            setTimer();
            while (rs.next()) {
                /* set binds for Insert */
                for (int i = 0; i < stbStatement.getMetaData().getColumnCount(); i++) {
                    int type = stbStatement.getMetaData().getColumnType(i + 1);
                    if (type == 93)/* timestamp*/ {
                        prodStatement.setTimestamp(i + 1, rs.getTimestamp(i + 1));
                    } else if (type == 2) /*NUMERIC*/ {
                        prodStatement.setInt(i + 1, rs.getInt(i + 1));
                    } else if (type == 12) /*VARCHAR*/ {
                        prodStatement.setString(i + 1, rs.getString(i + 1));
                    } else {
                        log.fatal("Unknown Data type" + type);
                    }

                }
                /* execute Insert */
                prodStatement.executeUpdate();
            }
            log.info("Stop inserting records" + getElapsedTime());
            log.info("Commiting");
            /* commit */
            prod.commit();

        } catch (Exception e) {
            log.fatal(e);
            try {
                prod.rollback();
                stb.rollback();
                prod.rollback();
                stb.rollback();
            } catch (Exception ex) {
                log.fatal(ex);
            }
        } finally {
            try {
                if (stbStatement != null) {
                    /* close connections */
                    stbStatement.close();
                    stb.close();
                }

                if (prodStatement != null) {
                    /* close connections */
                    prodStatement.close();
                    prod.close();
                }

            } catch (Exception e) {
                log.fatal(e);
            }
        }
    }

    /* begin_work_com date */
    void updateBWC_Date(java.util.Date d, boolean dbms_out) {

        PreparedStatement stbStatement = null;
        PreparedStatement prodStatement = null;
        try {
            Connection prod = establishConn(prodURL, prodUser, prodPWD);
            Connection stb = establishConn(stbURL, stbUser, stbPWD);

            String SQL = "select /*+parallel(32)*/ \n"
                    + "                             Contour.Get_Begin_Work_Com(t.Subs_Id,1), t.subs_id\n"
                    + "                        from contour.t2_active_subs t \n"
                    + "                       where t.subs_id not in (select c_subs_id from T2_SUBS_BEGIN_WORK_COM)";

            String DML = "insert into T2_SUBS_BEGIN_WORK_COM values(?,?)\n";

            /* Turn off autocommit on prod database */
            prod.setAutoCommit(false);
            /* prepare Query statement for standby  */
            stbStatement = stb.prepareStatement(SQL);
            stbStatement.setFetchSize(10000);

            log.info("Start executing Query");
            ResultSet rs = stbStatement.executeQuery();
            /* Fetch rows */
            log.info("Start inserting records");
            while (rs.next()) {
                /* set binds for Insert */
                prodStatement.setInt(1, rs.getInt(1));
                prodStatement.setTimestamp(2, rs.getTimestamp(2));
                /* execute Insert */
                prodStatement.executeUpdate();
            }
            prod.commit();
            prod.close();
        } catch (Exception e) {
            log.fatal(e);
        }

    }

    void initialize() {
        try {
            log4jPath = xml.getNodeValue(xml.getChildNodes("parameters"), "log4jPath");
            PropertyConfigurator.configure(this.log4jPath);
            prodURL = xml.getNodeValue(xml.getChildNodes("parameters"), "prodURL");
            prodUser = xml.getNodeValue(xml.getChildNodes("parameters"), "prodUSER");
            prodPWD = xml.getNodeValue(xml.getChildNodes("parameters"), "prodPWD");

            stbURL = xml.getNodeValue(xml.getChildNodes("parameters"), "stbURL");
            stbUser = xml.getNodeValue(xml.getChildNodes("parameters"), "prodUSER");
            stbPWD = xml.getNodeValue(xml.getChildNodes("parameters"), "prodPWD");
            loadOJDBCDriver();
        } catch (Exception e) {
            System.err.println(e);
        }
    }

    String read_sql(String file) {
        BufferedReader br = null;
        File f = new File(file);
        String line = "-1", Result = "";
        // if file does not exist , file error
        if (!f.exists()) {
            return new FileNotFoundException().getMessage();
        }
        try {
            br = new BufferedReader(new FileReader(f));
            while ((line = br.readLine()) != null) {
                //System.out.println(i + "; " + line);
                Result += line + "\n";
            }
        } catch (Exception ex) {
            log.info(ex);
        }
        return Result;
    }

    Connection establishConn(String url, String user, String pwd) {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, user, pwd);
        } catch (SQLException ex) {
            log.fatal(ex);
        } finally {
            return conn;
        }
    }

    /*
     load Oracle ojdbc driver
     */
    void loadOJDBCDriver() {
        try {
            Driver myDriver = new oracle.jdbc.driver.OracleDriver();
            DriverManager.registerDriver(myDriver);
        } catch (Exception ex) {
            log.fatal("Error: unable to load driver class!");
            System.exit(1);
        }
    }

    /*
     forms bids list
     for example SQL 'select :1, :2, :3, :4 from dual' - will be turned into :1,:2,:3,:4
     */
    String getIntoBinds(ResultSetMetaData metaData) {
        String bids = "";
        int bid = 1;
        try {
            for (int i = 0; i < metaData.getColumnCount(); i++) {
                bids += ":" + bid;
                bid++;
                if (i + 1 < metaData.getColumnCount()) {
                    bids += ",";
                }
            }
        } catch (Exception e) {
            log.fatal(e);
        }
        return bids;
    }

    void setTimer() {
        this.startTime = System.currentTimeMillis();
    }

    String getElapsedTime() {
        return " Time spent " + new DecimalFormat("0.00").format((double) ((System.currentTimeMillis() - this.startTime) / 1000) / 60).toString() + " minutes";
    }

}
