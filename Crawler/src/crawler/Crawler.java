package crawler;

/*
++ HTML Download

Crawler Missing: 
Frequency Control
Proper Seeds
Termination Criteria
Multi-Threading

--Indexer
*/

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.SQLException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.FileUtils;
import org.jsoup.HttpStatusException;
import org.jsoup.UnsupportedMimeTypeException;



public class Crawler extends Thread
{

    private DatabaseManager CrawlerDBM=null;
    public int ID;
    
    public Crawler(DatabaseManager DB)
    {
        this.CrawlerDBM=DB;
    }
    
    /*
     public static void main(String[] args) {
         System.out.println("Enter the Number of Crawler Threads (20 Max): ");
         Scanner sc =new Scanner(System.in);
         int ThreadNumber=sc.nextInt();
         DatabaseManager MyDB=new DatabaseManager();
         Thread[] ThreadArray=new Thread[ThreadNumber];
         for(int i =0;i<ThreadNumber;i++)
         {
             ThreadArray[i] = new Thread (new Crawler(MyDB)); ThreadArray[i].setName ("Crawler "+ i);
             ThreadArray[i].start();
         }
        
    }
     */
     
    @Override
     public void run()
     {
        try 
        {
            Crawl();
        } catch (IOException ex) {
            Logger.getLogger(Crawler.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(Crawler.class.getName()).log(Level.SEVERE, null, ex);
        }
     }
    
    	public  void Crawl() throws IOException, SQLException //throws SQLException, IOException
        {
            String CurrentURL;
            int ID=0;
            int[] rID=new int[1];
            while(true)
            {
//		        Document doc = Jsoup.connect(MyURLs.elementAt(Index)).get();
//                      System.out.println(MyURLs.elementAt(Index));	
                       
                String Abs;
                synchronized(CrawlerDBM)
                {
                 if (CrawlerDBM.MaxIDReached())
                 { 
                     System.out.println(this.getName()+" has finished! ");
                     return;
                 }
                CurrentURL=CrawlerDBM.GetNotVisitedURL(rID);
                if(CurrentURL.equals(""))
                {
                   // flag=false;
                    break;
                }
                CrawlerDBM.SetVisited(CurrentURL, true);
                }
                ID=rID[0];
                if (CurrentURL.equals(""))
                {
                    System.out.println("Done...");
                    return;
                }
//                if(!MyDB.CheckRURL(CurrentURL))
  //              {
    //                    if(!MyDB.CheckURL(CurrentURL))
      //                  {
                        CheckRobotRestrictions(CurrentURL);
                        try
                        {
                        Document doc = Jsoup.connect(CurrentURL).get();
                        DownloadHTML(doc, ID);
                        synchronized(CrawlerDBM)
                        {
                        CrawlerDBM.SetDownloaded(CurrentURL, true);
                        }
                        
                            System.out.print(Thread.currentThread().getName()+": ");
                        System.out.println(CurrentURL);
                        //MyDB.AddURL(CurrentURL);
                        //Download HTML
                        Elements questions = doc.select("a[href]");
                        int LinksCount=0; //counts the number of urls that a page points to
			for(Element link: questions)
                         {
                             Abs=link.attr("abs:href");
                             synchronized(CrawlerDBM)
                             {
                              if(!CrawlerDBM.CheckRURL(Abs))
                              {
                                  LinksCount++;
                                  if(!CrawlerDBM.CheckURL(Abs))
                                  {
                                       CrawlerDBM.AddURL(Abs);
                                  }
                              }
                             }
                             if (LinksCount>=50)
                             {
                                 synchronized(CrawlerDBM){
                                     CrawlerDBM.SetPriority(CurrentURL, true);
                                 }
                             }
			
			 }
//                        synchronized(CrawlerDBM)
//                        {
//                        CrawlerDBM.SetVisited(CurrentURL, true);
//                        }
                        }
                        catch(UnsupportedMimeTypeException Type)
                        {
                          System.out.println("Unsupported Type...");
                          //MyDB.20(CurrentURL);
                          synchronized(CrawlerDBM)
                          {
                          CrawlerDBM.DeleteURL(CurrentURL);
                          }
                        }
                        catch(HttpStatusException Hex)
                        {
                          System.out.println("Page Not Found...");
                          synchronized(CrawlerDBM)
                          {
                          CrawlerDBM.DeleteURL(CurrentURL);
                          }
                        }
           if(this.isInterrupted())
           {
               System.out.println(this.getName()+" Terminated");
              break;
           }           
            }
        }     
   
        public void CheckRobotRestrictions(String MyURL) throws IOException, SQLException
        {
            String str;
            String RURL;
            InputStream inStream;
        try 
        {
            URL url =new URL(MyURL+"/robots.txt");
            inStream=url.openStream();
            InputStreamReader inStreamReader=new InputStreamReader(inStream);
            BufferedReader in=new BufferedReader(inStreamReader);
            String Arr[];
            
            str=in.readLine();
            
            if(str==null)
            {
                return;
            }  
                if(str.equals("User-agent: *"))
                {
                    while(str!=null)
                    {
                    if (str.contains("Disallow:"))
                    {
                        Arr=str.split(" ");
                        RURL=MyURL+Arr[1];
                        
                        synchronized(CrawlerDBM)
                        {
                        if(!CrawlerDBM.CheckRURL(RURL))
                        CrawlerDBM.AddRURL(RURL);
                        }
                    }
                    str=in.readLine();
                    } 
                }
        } 
        catch(FileNotFoundException nf)//For Testing And Clarity
        {
            System.out.println("No Robot File");
        }
        catch(IOException IO)//For Testing And Clarity
        {
            //System.out.println("Access Denied... "+MyURL+" Added To Restricted URLs");
            synchronized(CrawlerDBM)
            {
             if(!CrawlerDBM.CheckRURL(MyURL+"/robots.txt"))
            CrawlerDBM.AddRURL(MyURL+"/robots.txt");
            }
        }
        catch (Exception ex) 
        {
            Logger.getLogger(Crawler.class.getName()).log(Level.SEVERE, null, ex);
        }   
        }
        
        public void DownloadHTML(Document MyDoc,int ID) throws IOException
        {
           File MyFile=new File(ID+".html");
           FileUtils.writeStringToFile(MyFile, MyDoc.outerHtml(),"UTF-8");
        }
}
    

