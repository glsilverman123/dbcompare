import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Created by glennsilverman on 4/21/15.
 */
public class Main {

    private static final String SELECT_SITES = "SELECT site_name, site_number from sites";
    private static final String SELECT_PATIENTS = "Select dtype,birth_date,enrollment_number,has_drug_related_case,"
            +"post_processing_pending,screening_number,study_state,initials,updated from patients";

    //private static final String SELECT_CASES = "SELECT accepted,priority,safety_id FROM cases";

    private static final String SELECT_CASE_VERSIONS = "SELECT active, agreement,assessed, criticality, name, list_status,"
            +"list_status_reason,action_taken, auto_narrative,broader_term, clinical_course, company_causality, country,"
            +"course_treatment, dictionary_term,discharge_diagnosis,dob,drug_related,event,evidence_supporting_recovery,"
            +"followup_course,is_initial,investigator_causality,life_threatening,number_events,other_drug_related,primary_event,"
            +"race,screening_number,signs_symptoms,site_name,submission_response,submission_status,target,type,type_delivery_termination,"
            +"md5(narrative),serious,state,status,transmission"
            +" FROM case_versions ORDER BY md5(narrative)";

    private static final String SELECT_ICSR = "SELECT md5(value) FROM icsr_documents ORDER BY md5(value)";

    private static final String[] selectQueries = {SELECT_SITES,SELECT_PATIENTS, SELECT_CASE_VERSIONS, SELECT_ICSR};
    private static final String[] files = {"sites","patients", "caseversions", "icsrdocs"};

    public static void main (String[] args) {
        Connection conn1 = null;
        Connection conn2 = null;

        String host1 = null;
        String db1 = null;
        String url1 = null;
        String userName1 = null;
        String password1 = null;

        String host2 = null;
        String db2 = null;
        String url2 = null;
        String userName2 = null;
        String password2 = null;

        if(args == null){
            System.out.println("No arguments");
            return;
        }

        String [] args1 = args[0].split(":");
        if(args1.length > 0) {
            host1 = args1[0];
            db1 = args1[1];
            url1 = String.format("jdbc:mysql://%s/%s", host1, db1);
            userName1 = args1[2];
            if(args1.length > 3)
                password1 = args1[3];

            conn1 = connect(url1, userName1, password1);
        }

        if(args.length > 1) {
            String[] args2 = args[1].split(":");
            if (args2.length == 4) {
                host2 = args2[0];
                db2 = args2[1];
                url2 = String.format("jdbc:mysql://%s/%s", host2, db2);
                userName2 = args2[2];
                if (args2.length > 3)
                    password2 = args2[3];

                conn2 = connect(url2, userName2, password2);
            }
        }


        try {
            writeToFiles(conn1, selectQueries, db1);

            if(conn2 != null) {
                writeToFiles(conn2, selectQueries, db2);


                for(String file : files)
                    fileCompare(file, db1, db2);

            }



        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            close(conn1);
            close(conn2);
       }

    }

    private static void writeToFiles(Connection conn, String[] queries, String db) throws Exception {
        List<ResultSet> rslist = new ArrayList<ResultSet>();
        for(String query : queries)
            rslist.add(exportTable(conn, query, db));

        int i = 0;
        for(String f : files)
             convertToCsv(rslist.get(i++), f, db);
    }

    private static void deleteFile(String fileName) {

        boolean bool = new File("/tmp/" + fileName).exists();
        if(bool) {
            try {
                Files.delete(FileSystems.getDefault().getPath("/tmp/", fileName));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static Connection connect(String url, String username, String password){

        Connection conn = null;
        try {
            Class.forName ("com.mysql.jdbc.Driver").newInstance ();
            conn = DriverManager.getConnection(url, username, password);
            System.out.println ("Connected");
        }
        catch (Exception e) {
            System.err.println ("Cannot connect to server");
            close(conn);
            System.exit (1);
        }

        return conn;

    }

    private static void close(Connection conn){
        if (conn != null)
        {
            try
            {
                conn.close ();
                System.out.println ("Disconnected");
            }
            catch (Exception e) { /* ignore close errors */ }
        }
    }

    public static ResultSet exportTable(Connection conn, String query, String db)
            throws SQLException {

        Statement stmt = null;
        ResultSet res =null;

        try {

            stmt = conn.createStatement();
            res = stmt.executeQuery(String.format(query, db));

        } catch (SQLException e ) {
            System.out.println("Error: " + e.getMessage());
        } finally {
            //if (stmt != null) { stmt.close(); }
        }

        return res;
    }

    private static String fileToString(String fileName) {
        String dataToPrint = "";
        BufferedReader input = null;
        FileReader fr = null;

        try {
             fr = new FileReader(fileName);
            input = new BufferedReader(fr);
            String line = "";
            /**Read the file and populate the data**/
            while ((line = input.readLine()) != null) {
                dataToPrint += line + "\n";
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally{
            try {
                input.close();
                fr.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return dataToPrint;
    }

    private static void fileCompare(String file, String db1, String db2) throws IOException{
        String fn1 = file + "-" + db1 + ".csv";
        String fn2 = file + "-" + db2 + ".csv";

        FileInputStream is1 = new FileInputStream("/tmp/" + fn1);
        FileInputStream is2 = new FileInputStream("/tmp/" + fn2);
        Scanner sc1 = new Scanner(is1);
        Scanner sc2 = new Scanner(is2);
        System.out.println("*********** Comparing " + fn1 + ":" + fn2);
        int i = 0;
        while (sc1.hasNext() && sc2.hasNext()) {
            String str1 = sc1.next();
            String str2 = sc2.next();
            if (!str1.equals(str2))
                System.out.println("   " + i++ + ")" + str1 + " != " + str2);
        }

        while(sc1.hasNext()) {
            sc1.next();
            i++;
        }

        int j = 0;
        while(sc2.hasNext()) {
            sc2.next();
            j++;
        }

        if(i < j)
            System.out.println(String.format("  %s has %d more records than %s", fn2, j - i, fn1));
        if(j < i)
            System.out.println(String.format("  %s has %d more records than %s", fn1, i - j, fn2));

        sc1.close();
        sc2.close();
    }

    private static void convertToCsv(ResultSet rs, String fileName, String db) throws SQLException, FileNotFoundException {
        PrintWriter csvWriter = new PrintWriter(new File("/tmp/" + fileName + "-" + db + ".csv")) ;

        ResultSetMetaData meta = rs.getMetaData() ;
        int numberOfColumns = meta.getColumnCount() ;
        String dataHeaders = "\"" + meta.getColumnName(1) + "\"" ;
        for (int i = 2 ; i < numberOfColumns + 1 ; i ++ ) {
            dataHeaders += ",\"" + meta.getColumnName(i) + "\"" ;
        }
        csvWriter.println(dataHeaders) ;
        while (rs.next()) {
            String row = "\"" + rs.getString(1) + "\""  ;
            for (int i = 2 ; i < numberOfColumns + 1 ; i ++ ) {
                row += ",\"" + rs.getString(i) + "\"" ;
            }
            csvWriter.println(row) ;
        }
        csvWriter.close();
    }

}
