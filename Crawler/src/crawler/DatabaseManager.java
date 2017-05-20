package crawler;

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

    public DatabaseManager() throws PropertyVetoException 
    {
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

        if(rs.next())
        {
            Check=rs.getInt(1)>0;
        }
        else
        {
            Check=false;
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
    //adding seeds to the table of URLs in DB
    public void AddSeeds() throws SQLException {
        String[] SURL = new String[8];

        SURL[0] = "https://en.wikipedia.org/wiki/Main_Page";
        SURL[1] = "http://www.independent.co.uk/";
        SURL[2] = "https://www.amazon.com/";
        SURL[3] = "http://www.imdb.com/";
        SURL[4] = "http://web.mit.edu/";
        SURL[5] = "http://www.bbc.com/news/magazine";
        SURL[6] = "https://www.gamespot.com/";
        SURL[7] = "http://stackoverflow.com";

        //adding each element in the array of strings of seeds to the database to the table of URLs
        for (int i = 0; i < SURL.length; i++) {
            if (!CheckURL(SURL[i])) //to ensure that this URL does not already exist in the DB
            {
                AddURL(SURL[i]);
            }
        }
    }

    //==Controller Methods/////////////////////////////////////////
    //Popularity is the number of links pointing to a URL "Default"=1
    public boolean IncPopularity(String URL) throws SQLException {
        //the boolean parameter is converted to a string to be compatible with the type of the flag in the DB
        String SQLVisited = "update URLData set Popularity = Popularity+1 where URL ='" + URL + "'";
        return (ExecuteSQL(SQLVisited));
    }

    //inserts the keyword, along with the URL it is found in with its frequency in that URL in the KeyData table in the DB
    public boolean AddKeyData(int KID, int URID, int Freq) throws SQLException {
        String SQLInsert = "Insert into KeyData(KID,UID,Frequency) values(" + KID + "," + URID + "," + Freq + "); ";

        return (ExecuteSQL(SQLInsert));
    }

    //inserts the position of a creatin keyword, along with its ID, into the "Ranking" table in DB
    public boolean AddPosition(int KDID, int Position) throws SQLException{
        String SQLInsert = "Insert into Ranking values(" + KDID + "," + Position + "); ";
          boolean result=false;
        try {
            //System.out.println(KDID+","+Position);
            result= (ExecuteSQL(SQLInsert));
        } catch (MySQLIntegrityConstraintViolationException ex) {
        }
        return result;
    }

    public boolean AddRanking(int KDID, int Position, int Tag) throws SQLException {
        String SQLInsert = "Insert into Ranking values(" + KDID + "," + Position + "," + Tag + "); ";
        return (ExecuteSQL(SQLInsert));
    }

    public boolean AddKeyWord(String Word) {
              Word=Word.replaceAll("'", "");

        String SQLInsert = "Insert into KeyWords(KeyWord) values('" + Word + "'); ";

        try {
            return (ExecuteSQL(SQLInsert));
        } catch (SQLException E) {
            return false;
        }
    }

    public boolean AddURL(String URL) throws SQLException {
             URL=URL.replaceAll("'", "");

     if(URL.equals(""))
     {
         return false;
     }
        String SQLInsert = "Insert into URLData(URL) values('" + URL + "'); ";

        return (ExecuteSQL(SQLInsert));
    }

    //Auto-increment functions/////////////////////////////////////
    public boolean ResetAutoIncKeyData() throws SQLException {
        String SQLInsert = "ALTER TABLE `searchengine`.`Keydata` AUTO_INCREMENT = 1;  ";

        return (ExecuteSQL(SQLInsert));
    }

    public boolean ResetAutoIncURLData() throws SQLException {
        String SQLInsert = "ALTER TABLE `searchengine`.`urldata` AUTO_INCREMENT = 1;  ";

        return (ExecuteSQL(SQLInsert));
    }

    public boolean ResetAutoIncRestricted() throws SQLException {
        String SQLInsert = "ALTER TABLE `searchengine`.`urldata` AUTO_INCREMENT = 1;  ";

        return (ExecuteSQL(SQLInsert));
    }

    public boolean ResetAutoIncKeywords() throws SQLException {
        String SQLInsert = "ALTER TABLE `searchengine`.`keywords` AUTO_INCREMENT = 1;  ";

        return (ExecuteSQL(SQLInsert));
    }

    ////////////////////////////////////////////////////////////////////////
    //Setting the flags functions by the boolean value sent to them//////////
    public boolean SetVisited(String URL, boolean V) throws SQLException {
        //the boolean parameter is converted to a string to be compatible with the type of the flag in the DB
        char X;
        if (V) {
            X = 'T';
        } else {
            X = 'F';
        }
        String SQLVisited = "update URLData set Visited='" + X + "'where URL='" + URL + "'";
        return (ExecuteSQL(SQLVisited));
    }

    public boolean SetDownloaded(String URL, boolean V) throws SQLException {
        //the boolean parameter is converted to a string to be compatible with the type of the flag in the DB
        char X;
        if (V) {
            X = 'T';
        } else {
            X = 'F';
        }
        String SQLVisited = "update URLData set Downloaded='" + X + "'where URL='" + URL + "'";
        return (ExecuteSQL(SQLVisited));
    }

    public boolean SetIndexed(String URL, boolean I) throws SQLException {
        //the boolean parameter is converted to a string to be compatible with the type of the flag in the DB
        char X;
        if (I) {
            X = 'T';
        } else {
            X = 'F';
        }
        String SQLIndexed = "update URLData set Indexed='" + X + "'where URL='" + URL + "'";
        return (ExecuteSQL(SQLIndexed));
    }

    public boolean SetPriority(String URL, boolean V) throws SQLException {
        //the boolean parameter is converted to a string to be compatible with the type of the flag in the DB
        char X;
        if (V) {
            X = 'H';
        } else {
            X = 'L';
        }
        String SQLVisited = "update URLData set Priority='" + X + "'where URL='" + URL + "'";
        return (ExecuteSQL(SQLVisited));
    }
    /////////////////////////////////////////////////////////////////////////////////////

    //A boolean function to ensure the keyword sent as a parameter is not in the DB to insert it in the indexer
    public boolean CheckKey(String Keyword) throws SQLException {
                Keyword=Keyword.replaceAll("'", "");

        String SQLSelect = "Select Key_ID from keywords where KeyWord='" + Keyword + "'; ";

        return CheckSQL(SQLSelect);
    }

    //retrieves the ID of a specific keyword
    public int GetKeyID(String Keyword) throws SQLException {
               Keyword=Keyword.replaceAll("'", "");

        String SQLSelect = "Select Key_ID from keywords where KeyWord='" + Keyword + "'; ";

        int Temp=(int)ScalarSQL(SQLSelect);
        return Temp;    //1 is the column index of the Key_ID
    }
    public int GetMaxDownloadedID() throws SQLException {
        String SQLSelect = "select max(URL_ID) from urldata where Downloaded='T';";
        Object Ob=ScalarSQL(SQLSelect);
        int Temp;
        if(Ob!=null)
        {
           Temp=(int)Ob;
        }
        else
        {
            Temp=0;
        }
        return Temp;    //1 is the column index of the Key_ID
    }
    //retrieves the ID of a specific keyword in a specific URL
    public int GetKeyDataID(int KID, int UID) throws SQLException {
        String SQLSelect = "Select KD_ID from keydata where UID=" + UID + " and KID=" + KID + ";";
        int Temp=(int)ScalarSQL(SQLSelect);
        return Temp;
    }

    //retireves the ID  of a specific URL
    //return type is boolean to check, when the function is used, if the URL exists or not
    public boolean CheckURL(String URL) throws SQLException {
        URL=URL.replaceAll("'", "");
        String SQLSelect = "Select URL_ID from URLData where URL='" + URL + "'; ";

        return CheckSQL(SQLSelect);
    }

    //deletes a specifc URL from the DB
    public boolean DeleteURL(String URL) throws SQLException {
        String SQLSelect = "Delete from URLData where URL='" + URL + "'; ";

        return ExecuteSQL(SQLSelect);
    }

    //inserts a URL to the table of restricted URLs in the DB
    public boolean AddRURL(String RURL) throws SQLException {
        String SQLInsert = "Insert into Restricted(rURL) values('" + RURL + "'); ";

        return (ExecuteSQL(SQLInsert));
    }

    public String GetURLByID(int ID) throws SQLException
    {
        String SQLInsert = "select URL from urldata where URL_ID='"+ID+"';";

        return (ScalarSQL(SQLInsert)).toString();

    }
    
    //retrieves the URL of a non-visited URL by checking its flag
    //It also gets the ID of that URL 
    public String GetNotVisitedURL(int[] ID) throws SQLException {
        String SQLInsert = "Select URL,URL_ID from URLData where Visited='F' limit 1;";

        //ResultSet RS;//LESA
        ArrayList<URStruct> Row = (ReadSQL(SQLInsert));
        
        if(Row.isEmpty())
        {
            //if there isn't any not visited URLs
            ID[0] = 0;
            return "";
        } else {
            ID[0] = (int)Row.get(0).IR; //the ID of the non-visited URL
            return (Row.get(0).URL);
        }
    }

    public ArrayList<URStruct> GetSimilarWords(String Word) throws SQLException {
        String SQLSelect = "select KeyWord,Key_ID from keywords where KeyWord like '%"+Word+"%';";

        return (ReadSQL(SQLSelect));
    }

    //retrieves the URL of a visited, downloaded and not indexed URL by checking its flag
    //It also gets the ID of that URL 
    public String GetNotIndexedURL(int[] ID) throws SQLException {
        String SQLInsert = "Select URL,URL_ID from URLData where Visited='T'and Downloaded='T' and Indexed='F' limit 1;";

         ArrayList<URStruct> Row = (ReadSQL(SQLInsert));
        
        if(Row.isEmpty())
        {
            //if there isn't any not Indexed URLs
            ID[0] = 0;
            return "";
        } else {
            ID[0] = (int)Row.get(0).IR; //the ID of the non-Indexed URL
            return (Row.get(0).URL);
        }
    }

    //checks if a certain restricted URL is already in the DB or not 
    public boolean CheckRURL(String RURL) throws SQLException {
        RURL=RURL.replaceAll("'", "");
        String SQLSelect = "Select RURL_ID from Restricted where RURL='" + RURL + "'; ";
        return CheckSQL(SQLSelect);
    }

    //Functions that delete the tables in the DB
    public boolean DeleteAllURLs() throws SQLException {
        String SQLSelect = "Delete from URLData";

        return ExecuteSQL(SQLSelect);
    }

    public boolean DeleteAllKeywords() throws SQLException {
        String SQLSelect = "Delete from keywords";

        return ExecuteSQL(SQLSelect);
    }

    public boolean DeleteAllRankings() throws SQLException {
        String SQLSelect = "Delete from ranking";

        return ExecuteSQL(SQLSelect);
    }

    public boolean DeleteAllRestricted() throws SQLException {
        String SQLSelect = "Delete from Restricted";

        return ExecuteSQL(SQLSelect);
    }

    public boolean DeleteAllkeydata() throws SQLException {
        String SQLSelect = "Delete from keydata";
        return ExecuteSQL(SQLSelect);
    }
    ///////////////////////////////////////////////////////////////////////
    //Ranker DB Functions
    public ArrayList<URStruct> GetSingleWordTF(String Word) throws SQLException {
        String SQLSelect = "select a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms)as'TF' from (select t1.URL,t2.Frequency,t1.URLTerms\n"
                + "from \n"
                + "(select URL,URL_ID,sum(Frequency)as 'URLTerms' from urldata, keydata\n"
                + "where URL_ID=UID and URL_ID in (select URL_ID from urldata, keydata, keywords where Key_ID=KID and URL_ID=UID and KeyWord='" + Word + "')\n"
                + "group by URL) t1\n"
                + "inner join\n"
                + "(select URL_ID,Frequency\n"
                + "from urldata, keydata, keywords\n"
                + "where Key_ID=KID and URL_ID=UID and KeyWord='" + Word + "') t2\n"
                + "on\n"
                + "t1.URL_ID = t2.URL_ID) a\n"
                + "order by TF desc;";

        return ReadSQL(SQLSelect);
    }

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
    String SQLSelect = "select rtemp"+RankerNumber+".URL_ID,rtemp"+RankerNumber+".URL,sum(TF)+(Popularity*0.2) from rtemp"+RankerNumber+",urldata where urldata.URL_ID=rtemp"+RankerNumber+".URL_ID group by rtemp"+RankerNumber+".URL_ID,rtemp"+RankerNumber+".URL order by sum(TF)+(Popularity*0.2) desc;";

        return ReadResult(SQLSelect);
    }

    public void DropTempTable(int RankerNumber) throws SQLException 
    {
        String SQLDrop = "drop table if exists RTemp"+RankerNumber+";";
        ExecuteSQL(SQLDrop);
    }
    ///////////////////////////////////////////////////////////////////////
    //A function that resets the DB by calling all the delete and auto-increment functions

    void ResetDB() throws SQLException {
        DeleteAllRankings();
        DeleteAllkeydata();
        DeleteAllURLs();
        DeleteAllRestricted();
        DeleteAllKeywords();

        ResetAutoIncKeyData();
        ResetAutoIncURLData();
        ResetAutoIncRestricted();
        ResetAutoIncKeywords();
    }
    //calls the function that deletes the keydata of a URL of high priority
    //and resets the flags of a URL  of high priority

    public boolean Recrawl() throws SQLException {
        PurgeHighPriority();
        String SQLVisited = "update urldata set Visited='F',Downloaded='F',Indexed='F' where Priority='H' ;";
        return (ExecuteSQL(SQLVisited));
    }
    //deletes the keydata of a URL of high priority

    public boolean PurgeHighPriority() throws SQLException {
        String SQLPurgeHighPriority = "Delete keydata from keydata where UID in (SELECT URL_ID FROM urldata where Priority='H');";
        return (ExecuteSQL(SQLPurgeHighPriority));
    }
    //a boolean function that gets the max number of IDs of URLs in the DB and returns true if the 
    //number of URLs is greater than or equal to 50000

    public boolean MaxIDReached() throws SQLException {
        long URLCount = 0;
        String SQLSelect = "Select count(*) from urldata; ";
        URLCount = (long)ScalarSQL(SQLSelect);
        return URLCount >= 50000;
    }
}
