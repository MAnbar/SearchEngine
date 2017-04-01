package crawler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
 
        public class DatabaseManager 
        { 
           public Connection connection = null;  


        public DatabaseManager()
        {
            String connectionString ="jdbc:MySQL://localhost:3306/SearchEngine"; 
        
                try 
                {  
                   Class.forName("com.mysql.jdbc.Driver");

                    System.out.println("Connecting to database...");

                   connection=DriverManager.getConnection(connectionString,"root","Asserbasha1");
        
                       
                   // connection = DriverManager.getConnection(connectionString);  
                }  
                catch (SQLException se) 
                {  
                    se.printStackTrace();  
                }  
                catch (Exception e) 
                {  
                    e.printStackTrace();  
                } 

        }
        
        
        public boolean ExecuteSQL(String sql) throws SQLException
        {
            Statement sta=connection.createStatement();
            return sta.execute(sql);
        }
        public void AddSeeds() throws SQLException
        {
            String[] SURL=new String[9];
            
            SURL[0]="https://en.wikipedia.org/wiki/Main_Page";
            SURL[1]="http://stackoverflow.com/";
            SURL[2]="https://www.amazon.com/";
            SURL[3]="http://www.imdb.com/";
            SURL[4]="http://web.mit.edu/";
            SURL[5]="http://web.mit.edu/";
            SURL[6]="http://www.bbc.com/news/magazine";
            SURL[7]="https://www.gamespot.com/";
            SURL[8]="http://www.independent.co.uk/";

            for (int i=0;i<SURL.length;i++)
            if(!CheckURL(SURL[i]))
            {
              AddURL(SURL[i]);
            }
        }
        
        public ResultSet ReadSQL(String sql) throws SQLException
        {
            Statement sta=connection.createStatement();
            return sta.executeQuery(sql);
        }

        //==Controller Methods/////////////////////////////////////////
        public boolean AddKeyData(int KID,int URID,int Freq) throws SQLException
        {
            String SQLInsert="Insert into KeyData(KID,UID,Frequency) values("+KID+","+URID+","+Freq+"); ";
            
            return (ExecuteSQL(SQLInsert));
        }
        
        public boolean AddPosition(int KDID,int Position) throws SQLException
        {
            
            String SQLInsert="Insert into Ranking values("+KDID+","+Position+"); ";
            //System.out.println(KDID+","+Position);
            return (ExecuteSQL(SQLInsert));
        }
        
        public boolean AddKeyWord(String Word)
        {
            String SQLInsert="Insert into KeyWords(KeyWord) values('"+Word+"'); ";
            
            try
            {
            return (ExecuteSQL(SQLInsert));
            }
            catch(SQLException E)
            {
                return false;
            }
        }
        
        public boolean AddURL(String URL) throws SQLException
        {
            String SQLInsert="Insert into URLData(URL) values('"+URL+"'); ";
            
            return (ExecuteSQL(SQLInsert));
        }

        public boolean ResetAutoIncKeyData () throws SQLException
        {
            String SQLInsert="ALTER TABLE `searchengine`.`Keydata` AUTO_INCREMENT = 1;  ";
            
            return (ExecuteSQL(SQLInsert));
        }
        
        public boolean ResetAutoIncURLData () throws SQLException
        {
            String SQLInsert="ALTER TABLE `searchengine`.`urldata` AUTO_INCREMENT = 1;  ";
            
            return (ExecuteSQL(SQLInsert));
        }

        public boolean ResetAutoIncRestricted () throws SQLException
        {
            String SQLInsert="ALTER TABLE `searchengine`.`urldata` AUTO_INCREMENT = 1;  ";
            
            return (ExecuteSQL(SQLInsert));
        }
                
        public boolean ResetAutoIncKeywords () throws SQLException
        {
            String SQLInsert="ALTER TABLE `searchengine`.`keywords` AUTO_INCREMENT = 1;  ";
            
            return (ExecuteSQL(SQLInsert));
        }
        public boolean SetVisited(String URL, boolean V) throws SQLException
        {
            char X;
            if(V)
            {
                X='T';
            }
            else
            {
                X='F';
            }
            String SQLVisited="update URLData set Visited='"+X+"'where URL='"+URL+"'";    
            return (ExecuteSQL(SQLVisited));
        }
        
        public boolean SetDownloaded(String URL, boolean V) throws SQLException
        {
            char X;
            if(V)
            {
                X='T';
            }
            else
            {
                X='F';
            }
            String SQLVisited="update URLData set Downloaded='"+X+"'where URL='"+URL+"'";    
            return (ExecuteSQL(SQLVisited));
        }
        
        public boolean SetIndexed(String URL, boolean I) throws SQLException
        {
            char X;
            if(I)
            {
                X='T';
            }
            else
            {
                X='F';
            }
            String SQLIndexed="update URLData set Indexed='"+X+"'where URL='"+URL+"'";    
            return (ExecuteSQL(SQLIndexed));
        }
        
        public boolean SetPriority(String URL, boolean V) throws SQLException
        {
            char X;
            if(V)
            {
                X='H';
            }
            else
            {
                X='L';
            }
            String SQLVisited="update URLData set Priority='"+X+"'where URL='"+URL+"'";    
            return (ExecuteSQL(SQLVisited));
        }
        
        public boolean CheckKey(String Keyword) throws SQLException
        {
            String SQLSelect="Select Key_ID from keywords where KeyWord='"+Keyword+"'; ";
            
            return (ReadSQL(SQLSelect)).next();
        }
        
        public int GetKeyID(String Keyword) throws SQLException
        {
            String SQLSelect="Select Key_ID from keywords where KeyWord='"+Keyword+"'; ";
            //return (ReadSQL(SQLSelect)).getInt("Key_ID");
            ResultSet RS=ReadSQL(SQLSelect);
            
            RS.next();
            return  RS.getInt(1);    
        }
        
        public int GetKeyDataID(int KID,int UID) throws SQLException
        {
            String SQLSelect="Select KD_ID from keydata where UID="+UID+" and KID="+KID+";";
            //return (ReadSQL(SQLSelect)).getInt("Key_ID");
            ResultSet RS=ReadSQL(SQLSelect);
            
            if(!RS.next())
            {
                return 0;
            }
            
            return  RS.getInt("KD_ID");    
        }
        
        
        
        public boolean CheckURL(String URL) throws SQLException
        {
            String SQLSelect="Select URL_ID from URLData where URL='"+URL+"'; ";
            
            return (ReadSQL(SQLSelect)).next();
        }
        
        public boolean DeleteURL(String URL) throws SQLException
        {
            String SQLSelect="Delete from URLData where URL='"+URL+"'; ";
            
            return ExecuteSQL(SQLSelect);
        }
        
        public boolean AddRURL(String RURL) throws SQLException
        {
            String SQLInsert="Insert into Restricted(rURL) values('"+RURL+"'); ";
            
            return (ExecuteSQL(SQLInsert));
        }
        
        public String GetNotVisitedURL(int[] ID) throws SQLException
        {
            String SQLInsert="Select URL_ID,URL from URLData where Visited='F' limit 1;";
            
            ResultSet RS;
               RS = (ReadSQL(SQLInsert));
              
            if(RS.next())
            {
              ID[0]=RS.getInt("URL_ID");
              return (RS.getString("URL"));  
            }
            else
            {
                ID[0]=0;
                return "";
            }      
        }
        
        public String GetNotIndexedURL(int[] ID) throws SQLException
        {
            String SQLInsert="Select URL_ID,URL from URLData where Visited='T'and Downloaded='T' and Indexed='F' limit 1;";
            
            ResultSet RS;
               RS = (ReadSQL(SQLInsert));
              
            if(RS.next())
            {
              ID[0]=RS.getInt("URL_ID");
              return (RS.getString("URL"));  
            }
            else
            {
                ID[0]=0;
                return "";
            }      
        }
        
        public boolean CheckRURL(String RURL) throws SQLException
        {
            String SQLSelect="Select RURL_ID from Restricted where RURL='"+RURL+"'; ";
            
            return (ReadSQL(SQLSelect)).next();
        }
        

        
        public void CloseConnection()
        {
                    if (connection != null) 
                    {    try 
                    { connection.close(); } 
                     catch(Exception e) 
                        {} 
                    }
        }
        
         public boolean DeleteAllURLs() throws SQLException
        {
            String SQLSelect="Delete from URLData";
            
            return ExecuteSQL(SQLSelect);
        }
         
            public boolean DeleteAllKeywords() throws SQLException
        {
            String SQLSelect="Delete from keywords";
            
            return ExecuteSQL(SQLSelect);
        }
            
        public boolean DeleteAllRankings() throws SQLException
        {
            String SQLSelect="Delete from ranking";
            
            return ExecuteSQL(SQLSelect);
        }
            
               public boolean DeleteAllRestricted() throws SQLException
        {
            String SQLSelect="Delete from Restricted";
            
            return ExecuteSQL(SQLSelect);
        }
               
                  public boolean DeleteAllkeydata() throws SQLException
        {
            String SQLSelect="Delete from keydata";
            return ExecuteSQL(SQLSelect);
        }
                                   
        void ResetDB() throws SQLException
        {
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
                
        public boolean Recrawl() throws SQLException
        {
            PurgeHighPriority();
            String SQLVisited="update urldata set Visited='F',Downloaded='F',Indexed='F' where Priority='H' ;";    
            return (ExecuteSQL(SQLVisited));
        }
        
        public boolean PurgeHighPriority() throws SQLException
        {
        String SQLPurgeHighPriority="Delete keydata from keydata where UID in (SELECT URL_ID FROM urldata where Priority='H');";
        return (ExecuteSQL(SQLPurgeHighPriority));
        }
        
        
        public boolean MaxIDReached() throws SQLException
        {
            int maxID=0;
             String SQLSelect="Select MAX(URL_ID) AS URL_ID from urldata; ";
             ResultSet rs=ReadSQL(SQLSelect);
             rs.next();
             maxID=rs.getInt("URL_ID");
             if (maxID>=50000) return true;
             else return false;
        }
    
       }

