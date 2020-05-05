import Models.CustomObject;

import java.io.*;
import java.sql.*;
import java.util.SortedSet;
import java.util.TreeSet;

public class Task2 {
    private static SortedSet<CustomObject> all;

    public static void main(String[] args) {
        readDataFromFile();
        writeData();
        readDataFromDB();
    }

    public static void readDataFromFile() {
        all = new TreeSet<>();
        try {
            System.out.println("Opening file....");
            File file = new File("./fo_random.txt");
            FileReader fileReader = new FileReader(file);
            BufferedReader br = new BufferedReader(fileReader);
            //skips the first line
            br.readLine();
            System.out.println("Reading line by line.....");
            for (String line; (line = br.readLine()) != null;) {
                String[] parts = line.split("\\|");
                CustomObject co = new CustomObject(parts[0], parts[1], parts[2], parts.length == 4 ? parts[3] : null);
                all.add(co);
            }
            System.out.println("All lines were read.");
            br.close();
            fileReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeData() {
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement ps = null;
        try {
            Class.forName("org.h2.Driver");
            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection("jdbc:h2:./fo-data", "sa", "");
            System.out.println("Connection to H2 has been established.");

            String dropTbl = "DROP TABLE IF EXISTS fo";
            String createTbl = "CREATE TABLE IF NOT EXISTS fo(" +
                    "pk IDENTITY PRIMARY KEY," +
                    "MATCH_ID varchar NOT NULL," +
                    "MARKET_ID INT NOT NULL," +
                    "OUTCOME_ID varchar NOT NULL," +
                    "SPECIFIERS varchar," +
                    "DATE_INSERT bigint NOT NULL);";

            System.out.println("Droping table if exists.....");
            stmt = conn.createStatement();
            stmt.executeUpdate(dropTbl);

            System.out.println("Creating table in given database...");
            int result = stmt.executeUpdate(createTbl);
            System.out.println("Created table in given database...");

            /* batch insert */

            String query = "INSERT INTO fo (MATCH_ID, MARKET_ID, OUTCOME_ID, SPECIFIERS, DATE_INSERT) VALUES (?,?,?,?,?)";
            conn.setAutoCommit(true);
            ps = conn.prepareStatement(query);
            System.out.println("Preparing inserts");

            for (CustomObject co : all) {
                ps.setString(1, co.getMatch_id());
                ps.setInt(2, co.getMarket_id());
                ps.setString(3, co.getOutcome_id());
                ps.setString(4, co.getSpecifiers());
                ps.setLong(5, System.currentTimeMillis());
                ps.addBatch();
            }
            int [] tmp = ps.executeBatch();
            System.out.println("inserted " + tmp.length);


        } catch (SQLException throwables) {
            System.out.println(throwables.getMessage());
            throwables.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
                if (ps != null) {
                    ps.close();
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }

    private static void readDataFromDB() {
        Connection conn = null;
        Statement stmt = null;
        try {
            Class.forName("org.h2.Driver");
            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection("jdbc:h2:./fo-data", "sa", "");
            System.out.println("Connection to H2 has been established.");
            stmt = conn.createStatement();

            String sqlMin = "SELECT * FROM fo WHERE DATE_INSERT = (SELECT MIN(DATE_INSERT) FROM fo)";
            String sqlMax = "SELECT * FROM fo WHERE DATE_INSERT = (SELECT MAX(DATE_INSERT) FROM fo)";

            ResultSet result = stmt.executeQuery(sqlMin);

            System.out.println("Printing valus with smallest time....");
            while (result.next()) {
                System.out.println(
                        result.getString("MATCH_ID") + "|" +
                        result.getInt("MARKET_ID") + "|" +
                        result.getString("OUTCOME_ID") + "|" +
                        result.getString("SPECIFIERS") + "|" +
                        result.getLong("DATE_INSERT")
                        );
            }

            result = stmt.executeQuery(sqlMax);

            System.out.println("Printing valus with biggest time....");
            while (result.next()) {
                System.out.println(
                        result.getString("MATCH_ID") + "|" +
                        result.getInt("MARKET_ID") + "|" +
                        result.getString("OUTCOME_ID") + "|" +
                        result.getString("SPECIFIERS") + "|" +
                        result.getLong("DATE_INSERT")
                        );
            }

        } catch (SQLException throwables) {
            System.out.println(throwables.getMessage());
            throwables.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }
}
