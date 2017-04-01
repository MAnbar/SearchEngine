package crawler;


import crawler.DatabaseManager;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.Scanner;


public class Main 
{
    public static void main(String[] args) throws SQLException, IOException 
    {   
                 DatabaseManager MyDB=new DatabaseManager();
                 Scanner sc =new Scanner(System.in);
                 BufferedReader consolereader = new BufferedReader (new InputStreamReader(System.in));

         int X; 
         String InputRURL="";
      do{ 
      System.out.println("Enter:\n1. to Add Restircted URLS \n2. to Start Crawling/Indexing \n3. to Reset Data \n4. to Exit \n>>");    
      X=sc.nextInt();
      if(X==1)
      {
          System.out.println("Enter a URL to Restrict:");
          InputRURL=consolereader.readLine();
             if(!MyDB.CheckRURL(InputRURL))
             {
                 MyDB.AddRURL(InputRURL);
             }
             else 
             {
                 System.out.println("URL is Already Restricted");
             }
      }
      else if(X==3)
      {
          System.out.println("Are You Sure You Want To Reset All Data (Enter 'reset' to confirm):");
          if(consolereader.readLine().equals("reset"))
          {
          MyDB.ResetDB();
              System.out.println("Data Reseted");
          }
          else
          {
              System.out.println("Reset Aborted");
          }
      }
      else if (X==4)
      {
          return;
      }
       }while(X!=2);
        
         System.out.println("Enter the Number of Crawler Threads (20 Max): ");

         int ThreadNumber=sc.nextInt();
         MyDB.AddSeeds();
         
         Thread[] ThreadArray=new Thread[ThreadNumber];
         for(int i =0;i<ThreadNumber;i++)
         {
             ThreadArray[i] = new Crawler(MyDB); ThreadArray[i].setName ("Crawler "+ i);
             ThreadArray[i].start();
         }
      Thread IndexerThread =new  Indexer(MyDB);IndexerThread.setName("Indexer");
      IndexerThread.start();
      
      do{ 
      System.out.println("Enter the 1 to Recrawl, 2 to Stop the Crawler \n>>");    
      X=sc.nextInt();
      if(X==1)
      {
          MyDB.Recrawl();
          System.out.println("Recrawling Successful");
      }
      else if (X==2)
      {
          System.out.println("Terminating Threads...Please Wait");
         for(int i =0;i<ThreadNumber;i++)
         {
             ThreadArray[i].interrupt();
         }
            IndexerThread.interrupt();
      }
       }while(X!=2);
      
      System.out.println("Main Thread Terminated..");
    }
    //
   //File I/O if File Exists Delete it
  //Seeds
    //GameSpot
    //MIT
    //Stack
    //Wiki
    //Amazon
    //BB Magazine
    //IMDB
   
}
