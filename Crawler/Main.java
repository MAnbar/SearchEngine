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
                 DatabaseManager MyDB=new DatabaseManager();//Create DB Object
                 Scanner sc =new Scanner(System.in);
                 BufferedReader consolereader = new BufferedReader (new InputStreamReader(System.in));

         int X; 
         String InputRURL="";//RURL stands for restricted URL
      do{//First Option List
         //to store the user choice
      System.out.println("Enter:\n1. to Add Restircted URLS \n2. to Start Crawling/Indexing \n3. to Reset Data \n4. to Exit \n>>");    
      X=sc.nextInt();
      //Add Restricted URLs
      if(X==1)
      {
          System.out.println("Enter a URL to Restrict:");
          InputRURL=consolereader.readLine();
             if(!MyDB.CheckRURL(InputRURL))//to make sure that that URL has not been added to the restricted URLs already
             {
                 MyDB.AddRURL(InputRURL);
             }
             else 
             {
                 System.out.println("URL is Already Restricted");
             }
      }
      //Reset data
      else if(X==3)
      {//Confirmation Check
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
        //Start crawling/indexing
         System.out.println("Enter the Number of Crawler Threads (20 Max): ");

         int ThreadNumber=sc.nextInt();
         MyDB.AddSeeds();
         //Run Crawlers
         Thread[] ThreadArray=new Thread[ThreadNumber];
         for(int i =0;i<ThreadNumber;i++)
         {//Start Crawler Threads And Name Them
             ThreadArray[i] = new Crawler(MyDB); ThreadArray[i].setName ("Crawler "+ i);
             ThreadArray[i].start();
         }
         //Start Indexer Thread
      Thread IndexerThread =new  Indexer(MyDB);IndexerThread.setName("Indexer");
      IndexerThread.start();
      
      do{//Second Option List (Available After Running The Crawlers)
      System.out.println("Enter the 1 to Recrawl, 2 to Stop the Crawler \n>>");    
      X=sc.nextInt();
      if(X==1)
      {
          MyDB.Recrawl();
          System.out.println("Recrawling Successful");
      }
      else if (X==2)
      {//Interrupt All Threads
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
}
