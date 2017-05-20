package My;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mysql.jdbc.exceptions.MySQLIntegrityConstraintViolationException;
import java.beans.PropertyVetoException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DatabaseManager {

    public ComboPooledDataSource dataSource = null;

    public DatabaseManager() throws PropertyVetoException {
        dataSource = new ComboPooledDataSource();
        dataSource.setDriverClass("com.mysql.jdbc.Driver");
        dataSource.setJdbcUrl("jdbc:MySQL://localhost:3306/SearchEngine");
        dataSource.setUser("root");
        dataSource.setPassword("Asserbasha1");
    }
    
    @Override
    public void finalize()
    {
        dataSource.close();
    }

    public boolean ExecuteSQL(String sql) throws SQLException {
        Connection connection = null;
        Statement sta = null;
        boolean response = false;
        try {
            connection = dataSource.getConnection();
            sta = connection.createStatement();
            response = sta.execute(sql);
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
            throw ex;
        }
        finally
        {
            try {
                if (sta != null)
                    sta.close();
                if (connection != null)
                    connection.close();
            } catch (SQLException ex) {
                Logger.getLogger(DatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return response;
    }
    public Object ScalarSQL(String sql ) throws SQLException
    {
        Connection connection = null;
        Statement sta = null;
        Object Scalar = null;
        ResultSet rs;
        try 
        {
            connection = dataSource.getConnection();
            sta = connection.createStatement();
            rs = sta.executeQuery(sql);
            rs.next();
        Scalar= rs.getObject(1); 
        rs.close();
        }
        finally
        {
            try 
            {
                if (sta != null)
                    sta.close();
                if (connection != null)
                    connection.close();
            } catch (SQLException ex) {
                Logger.getLogger(DatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return Scalar; 
    }
    public boolean CheckSQL(String sql ) throws SQLException
    {
        Connection connection = null;
        Statement sta = null;
        boolean Check = false;
        ResultSet rs;
        try 
        {
            connection = dataSource.getConnection();
            sta = connection.createStatement();
            rs = sta.executeQuery(sql);

        Check=rs.next(); 
        rs.close();
        }
        finally
        {
            try 
            {
                if (sta != null)
                    sta.close();
                if (connection != null)
                    connection.close();
            } catch (SQLException ex) {
                Logger.getLogger(DatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return Check; 
    }
    public float IDFSQL (String sql) throws SQLException
    {
        Connection connection = null;
        Statement sta = null;
        float TotalURLCount = 0;
        float RelevURLCount = 0;
        float Result=0;
        boolean Failed=false;
        ResultSet rs;
        try 
        {
            connection = dataSource.getConnection();
            sta = connection.createStatement();
            rs = sta.executeQuery(sql);

        if (rs.next()) {
            TotalURLCount = rs.getFloat(1);
        } else {
            Failed=true;
        }
        if (rs.next()) {
            RelevURLCount = rs.getFloat(1);
            if (RelevURLCount==0)
            {
                Failed=true;
            }
        } else {
            Failed=true;
        }
        
        if (!Failed)
        {
            Result=TotalURLCount/RelevURLCount;
        }
        else
        {
            Result=0;
        }
        rs.close();
        }
        finally
        {
            try 
            {
                if (sta != null)
                    sta.close();
                if (connection != null)
                    connection.close();
            } catch (SQLException ex) {
                Logger.getLogger(DatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
      return Result;
    }     
    public ArrayList<URStruct> ReadSQL(String sql) throws SQLException 
    {
        Connection connection = null;
        Statement sta = null;
        ArrayList<URStruct> List = new ArrayList<URStruct>();
        ResultSet rs;
        try 
        {
            connection = dataSource.getConnection();
            sta = connection.createStatement();
            rs = sta.executeQuery(sql);
            //
            
  
    String URL;
    float IR;
    while(rs.next())
    {
        URL=rs.getString(1);
        IR=rs.getFloat(2);
        URStruct Entry = new URStruct(URL, IR);
        List.add(Entry);
    }
           rs.close();
        }
        finally
        {
            try 
            {
                if (sta != null)
                    sta.close();
                if (connection != null)
                    connection.close();
            } catch (SQLException ex) {
                Logger.getLogger(DatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return List;
    }
    public ArrayList<ResultStruct> ReadResult(String sql) throws SQLException 
    {
        Connection connection = null;
        Statement sta = null;
        ArrayList<ResultStruct> List = new ArrayList<ResultStruct>();
        ResultSet rs;
        try 
        {
            connection = dataSource.getConnection();
            sta = connection.createStatement();
            rs = sta.executeQuery(sql);
    int UID;
    String URL;
    float IR;
    while(rs.next())
    {
        UID=rs.getInt(1);
        URL=rs.getString(2);
        IR=rs.getFloat(3);
        ResultStruct Entry = new ResultStruct(UID, URL, IR);
        List.add(Entry);
    }
           rs.close();
        }
        finally
        {
            try 
            {
                if (sta != null)
                    sta.close();
                if (connection != null)
                    connection.close();
            } catch (SQLException ex) {
                Logger.getLogger(DatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return List;
    }
    public ArrayList<Integer> ReadIntegers(String sql) throws SQLException 
    {
        Connection connection = null;
        Statement sta = null;
        ArrayList<Integer> List = new ArrayList<Integer>();
        ResultSet rs;
        try 
        {
            connection = dataSource.getConnection();
            sta = connection.createStatement();
            rs = sta.executeQuery(sql);
           
            
    Integer ID;
    while(rs.next())
    {
        ID=rs.getInt(1);
        List.add(ID);
    }
           rs.close();
        }
        finally
        {
            try 
            {
                if (sta != null)
                    sta.close();
                if (connection != null)
                    connection.close();
            } catch (SQLException ex) {
                Logger.getLogger(DatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return List;
    }
    //################################################################################

    public String GetURLByID(int ID) throws SQLException
    {
        String SQLInsert = "select URL from urldata where URL_ID='"+ID+"';";

        return (ScalarSQL(SQLInsert)).toString();

    }
    
    //retrieves the URL of a non-visited URL by checking its flag
    //It also gets the ID of that URL 
    public ArrayList<URStruct> GetSimilarWords(String Word) throws SQLException {
        String SQLSelect = "select KeyWord,Key_ID from keywords where KeyWord like '%"+Word+"%';";

        return (ReadSQL(SQLSelect));
    }
    ///////////////////////////////////////////////////////////////////////
    //Ranker DB Functions
    public float GetWordIDF(String Word) throws SQLException {
        String SQLSelect = "select count(*)\n"
                + "from urldata\n"
                + "union\n"
                + "select count(*)\n"
                + "from urldata, keydata, keywords\n"
                + "where Key_ID=KID and URL_ID=UID and KeyWord='"+Word+"';";

        return IDFSQL(SQLSelect);
    }

    public ArrayList<Integer> GetRelevDocs(String Word) throws SQLException {
        String SQLSelect = "select url_id from urldata,keydata,keywords where URL_ID=UID and Kid=Key_ID and KeyWord='"+Word+"';";
        return ReadIntegers(SQLSelect);
    }
    public void CreateTempTFIDFTable(String RankerName,String Word, float IDF)  
    {
Connection connection = null;
        Statement sta = null;
        
        try {
            connection = dataSource.getConnection();
            String query = "{CALL CreateTempTFIDFTable(?,?,?)}";
            CallableStatement stmt = connection.prepareCall(query);
            stmt.setString(1, RankerName);
            stmt.setString(2, Word);
            stmt.setFloat(3, IDF);
            stmt.execute();
            
        } catch (SQLException ex) {
            System.out.println(Word +" Not Found in Database..");
        }
        finally
        {
            try {
                if (sta != null)
                    sta.close();
                if (connection != null)
                    connection.close();
            } catch (SQLException ex) {
                Logger.getLogger(DatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public void InsertIntoTempTFIDFTable(String RankerName,String Word, float IDF)
    {

Connection connection = null;
        Statement sta = null;
        
        try {
            connection = dataSource.getConnection();
            String query = "{CALL InsertIntoTempTFIDFTable(?,?,?)}";
            CallableStatement stmt = connection.prepareCall(query);
            stmt.setString(1, RankerName);            
            stmt.setString(2, Word);
            stmt.setFloat(3, IDF);
            stmt.execute();
            
        } catch (SQLException ex) 
        {
            System.out.println(Word +" Not Found in Database..");
        }
        finally
        {
            try {
                if (sta != null)
                    sta.close();
                if (connection != null)
                    connection.close();
            } catch (SQLException ex) {
                Logger.getLogger(DatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public ArrayList<ResultStruct> GetMultipleTFIDF(int RankerNumber) throws SQLException {
        String SQLSelect = "select URL_ID,URL,Sum(TF) from RTemp"+RankerNumber+"\n"
                + "group by URL_ID,URL\n"
                + "order by sum(TF) desc;";

        return ReadResult(SQLSelect);
    }

    public void DropTempTable(int RankerNumber) throws SQLException 
    {
        String SQLDrop = "drop table if exists RTemp"+RankerNumber+";";
        ExecuteSQL(SQLDrop);
    }
}
