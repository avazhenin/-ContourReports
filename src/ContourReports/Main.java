/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ContourReports;

import java.text.DecimalFormat;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import javax.swing.text.DateFormatter;

/**
 *
 * @author vazhenin
 */
public class Main {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

        ArrayList<Test> test = new ArrayList<>();
        test.add(new Test("John", 1));
        test.add(new Test("John", 1));
        test.add(new Test("Peter", 1));
        test.add(new Test("Peter", 2));        

        try {
            new Tele2Reports(args[0]).T2_Active_Subscribers(new SimpleDateFormat("dd.MM.yyyy").parse("15.02.2016"), true);
//            new Tele2Reports(args[0]).T2_Active_Subscribers(new SimpleDateFormat("dd.MM.yyyy").parse("16.02.2016"), false);
//            new Tele2Reports(args[0]).T2_Active_Subscribers(new SimpleDateFormat("dd.MM.yyyy").parse("17.02.2016"), false);
//            new Tele2Reports(args[0]).T2_Active_Subscribers(new SimpleDateFormat("dd.MM.yyyy").parse("18.02.2016"), false);
//            new Tele2Reports(args[0]).T2_Active_Subscribers(new SimpleDateFormat("dd.MM.yyyy").parse("19.02.2016"), false);
//            new Tele2Reports(args[0]).updateBWC_Date(new SimpleDateFormat("dd.MM.yyyy").parse("17.02.2016"), false);
        } catch (Exception e) {
            System.err.println(e);
        }

    }

    static class Test {

        String name;
        int idx;

        public Test(String name, int idx) {
            this.name = name;
            this.idx = idx;
        }

        public void setName(String name) {
            this.name = name;
        }

        public void setIdx(int idx) {
            this.idx = idx;
        }

        public String getName() {
            return name;
        }

        public int getIdx() {
            return idx;
        }

    }

}
