package crawler;

import java.beans.PropertyVetoException;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Scanner;


public class Main 
{
    public static void main(String[] args) throws SQLException, IOException, PropertyVetoException, InterruptedException 
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
          int MaxDownloadedID =MyDB.GetMaxDownloadedID();
          try{
              for(int i=1;i<=MaxDownloadedID;i++)
              {
    		File file = new File(i+".html");
    		if(file.delete()){
    			System.out.println(file.getName() + " is deleted!");
    		}else{
    			System.out.println("Delete operation is failed.");
    		}
              }
    	}catch(Exception e){
    		e.printStackTrace();
    	}
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
         Thread[] CThreadArray=new Thread[ThreadNumber];
         Thread[] IThreadArray=new Thread[20];
         for(int i =0;i<ThreadNumber;i++)
         {//Start Crawler Threads And Name Them
             CThreadArray[i] = new Crawler(MyDB); 
             CThreadArray[i].setName ("Crawler "+ i);
         }
         for(int i=0;i<20;i++)
         {
             IThreadArray[i] =new  Indexer(MyDB);
             IThreadArray[i].setName("Indexer"+ i);
         }
         //System.out.println("Enter the 1 to Recrawl, 2 to Stop the Crawler \n>>");    
         for(int i =0;i<ThreadNumber;i++)
         {//Start Crawler Threads And Name Them
             CThreadArray[i].start();
         }
                  for(int i=0;i<20;i++)
         {
             IThreadArray[i].start();
         }
         //Start Indexer Thread
      
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
             CThreadArray[i].interrupt();
         }
         for(int i=0;i<20;i++)
         {
              IThreadArray[i].interrupt();
         }
         for(int i =0;i<ThreadNumber;i++)
         {
             CThreadArray[i].join();
         }
                  for(int i=0;i<20;i++)
         {
             IThreadArray[i].join();
         }
         
            
      }
       }while(X!=2);
      
      System.out.println("Main Thread Terminated..");
    }
    
//    public static void main(String[] args) throws SQLException, IOException, PropertyVetoException, InterruptedException 
//    {
//        DatabaseManager MyDB=new DatabaseManager();//Create DB Object
//        Ranker R=new Ranker(MyDB,1);
//        String W="gggfgfgfyfyfuhjhi game";
//        ArrayList<URStruct> List=R.GetURLRankings(W);
//        
//        for(int i=0;i<List.size();i++)
//        {
//            System.out.println("URL: "+List.get(i).URL+" IF: "+List.get(i).IR);
//        }
//    }
//        public static void main(String[] args) throws SQLException, IOException, PropertyVetoException, InterruptedException 
//    {
//         DatabaseManager MyDB=new DatabaseManager();
//           Scanner sc =new Scanner(System.in);
//           BufferedReader consolereader = new BufferedReader (new InputStreamReader(System.in));
//
//        String Word=consolereader.readLine();
//        QueryProcessor QP = new QueryProcessor(Word,MyDB,1);
//        ArrayList<ResultStruct> PTemp=new ArrayList<ResultStruct>();
//        PTemp=QP.ProcessQuery(Word);
//        if(PTemp==null)
//        {
//            System.out.println("Phrase Not Found"); 
//            return;
//        }
//        for(int i=0;i<PTemp.size();i++)
//        {
//           System.out.println(PTemp.get(i).UID+" "+PTemp.get(i).URL+" "+PTemp.get(i).Rating); 
//        }
////System.out.println(QP.Stem("Footballers"));
//    }
}
