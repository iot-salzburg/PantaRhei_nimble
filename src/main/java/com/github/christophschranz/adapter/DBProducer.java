package com.github.christophschranz.adapter;

import com.google.gson.JsonObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;


public class DBProducer {
    private String endpoint;
    private String user;
    private String password;

    private Connection db_connection = null;

    public DBProducer(String endpoint, String user, String password) {
        this.endpoint = endpoint;
        this.user = user;
        this.password = password;
    }


    public void produce(JsonObject datapoint) {};


    // ------------------------------------------------------------------------------------------------
    // opens a connection for this class (needs to be done first to work properly)
    // ------------------------------------------------------------------------------------------------
    public boolean connectToDatabase()
    {
        try {
            Class.forName("org.postgresql.Driver");
            this.db_connection = DriverManager.getConnection(this.endpoint, this.user, this.password);
            this.db_connection.setAutoCommit(true);

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName()+": "+e.getMessage());
            return false;
        }
        System.out.println("Opened database successfully");

        return true;
    }

    // ------------------------------------------------------------------------------------------------
    // creates the database only if connected already
    // ------------------------------------------------------------------------------------------------
    public boolean createDatabase()
    {
        if(db_connection == null)
        {
            return false;
        }

        Statement stmt = null;
        try {

            stmt = this.db_connection.createStatement();
            String sql = "CREATE TABLE IF NOT EXISTS Metadata (id integer NOT NULL PRIMARY KEY, quantity varchar(50) NOT NULL, unit varchar(15));";
            stmt.executeUpdate(sql);
            sql = "CREATE TABLE IF NOT EXISTS Streamdata (id integer NOT NULL, timestamp datetime NOT NULL, value REAL NOT NULL, PRIMARY KEY (id, timestamp));";
            stmt.executeUpdate(sql);
            sql = "CREATE OR REPLACE VIEW data AS SELECT id, timestamp, quantity, value, unit FROM metadata NATURAL JOIN streamdata;";
            stmt.executeUpdate(sql);
//            stmt.executeUpdate("DELETE FROM metadata;");
            stmt.close();
//            this.db_connection.commit();
//            this.db_connection.close();
        } catch ( Exception e ) {
            System.err.println( e.getClass().getName()+": "+ e.getMessage() );
            return false;
        }
        System.out.println("Table created successfully");
        return true;
    }

    // ------------------------------------------------------------------------------------------------
    // inserting one single entry with col definition to the database
    // ------------------------------------------------------------------------------------------------
    public boolean insertToDatabase(String tableName, HashMap<String, Object> colvalMap)
    {
        if(db_connection == null)
        {
            return false;
        }


        Statement stmt = null;
        try {
            String sql = "INSERT INTO " + tableName + " (";
            String sql_tail = "VALUES (";

            for(HashMap.Entry<String,Object> entry: colvalMap.entrySet())
            {
                sql += entry.getKey() + ",";
                sql_tail += entry.getValue() + ",";

            }
            sql = sql.substring(0, sql.length()-1);
            sql += ") ";

            sql_tail = sql_tail.substring(0, sql_tail.length()-1);
            sql_tail += ") ON CONFLICT DO NOTHING;";
            sql += sql_tail;

//            System.out.println("SQL: " + sql);

            stmt = this.db_connection.createStatement();
            stmt.executeUpdate(sql);
            stmt.close();
//            this.db_connection.commit();
//            this.db_connection.close();
//        } catch (org.postgresql.util.PSQLException e) {
//            return true;
        } catch (Exception e) {
            System.err.println( e.getClass().getName()+ ": "+ e.getMessage() );
            return false;
        }
//        System.out.println("Records created successfully");
        return true;
    }

}
