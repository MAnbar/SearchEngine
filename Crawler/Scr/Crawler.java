package crawler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.UnknownHostException;
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

/*
--
Connection Timeout Exeception Handeling {java.net.UnknownHostException}
Auto Recrawl Criteria
Robot Delay
Connection Pool Implementation

*/
public class Crawler extends Thread
{

    private DatabaseManager CrawlerDBM=null;
    //public int ID;
    
    public Crawler(DatabaseManager DB)
    {
        this.CrawlerDBM=DB;
    }
     
    @Override
     public void run()
     {
        try 
        {
            Crawl();//Real Run function
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
                String Abs;
                synchronized(CrawlerDBM)
                {
                 if (CrawlerDBM.MaxIDReached())//If Stopping Criteria (50,000 Links) Was Reached
                 { 
                     System.out.println(this.getName()+" has finished! ");//Crawler Thread Terminates
                     return;
                 }
                CurrentURL=CrawlerDBM.GetNotVisitedURL(rID);//Get First Not Visited (Fetched But Not Processed)
                if(CurrentURL.equals(""))//If All URLs Were Visited Crawler Thread Terminates
                {
                   System.out.println("Done...");
                   return;
                }
                CrawlerDBM.SetVisited(CurrentURL, true);//Set CurrentURL As Visited
                }
                ID=rID[0];//Set ID with CurrentURL ID

                        CheckRobotRestrictions(CurrentURL);//Add All Restricted Sub-Domains Into The Restricted List
                        try
                        {//Internet Disconnection ..
                        Document doc = Jsoup.connect(CurrentURL).get();//Open CurrentURL With JSoup
                        DownloadHTML(doc, ID);//Download HTML File Of CurrentURL
                        synchronized(CrawlerDBM)
                        {
                        CrawlerDBM.SetDownloaded(CurrentURL, true);//Set CurrentURL's Downloaded DB Flag As True
                        }
                        
                        System.out.print(Thread.currentThread().getName()+": ");
                        System.out.println(CurrentURL);//Display Crawler Information To The User

                        Elements questions = doc.select("a[href]");
                        int LinksCount=0; //Counts The Number Of URLs That a Page Points To
			for(Element link: questions)//For Every URL
                         {
                             Abs=link.attr("abs:href");//Get Absoulute URL of the URL
                             
                             //Remove Anchors
                             Abs=Abs.split("#")[0];
                             
                             synchronized(CrawlerDBM)
                             {
                              if(!CrawlerDBM.CheckRURL(Abs))//Is This Link Restricted ?
                              {//No
                                  LinksCount++;
                                  if(!CrawlerDBM.CheckURL(Abs))//Was This Link Extracted Before ?
                                  {//No
                                       CrawlerDBM.AddURL(Abs);//Add This Link To The DB
                                  }
                                  else
                                  {
                                      CrawlerDBM.IncPopularity(Abs);
                                  }
                              }
                             }
                             if (LinksCount>=50)//Our Criteria For High Priority URLs
                             {
                                 synchronized(CrawlerDBM)
                                 {
                                     CrawlerDBM.SetPriority(CurrentURL, true);//Set URL Priority As High
                                                                              //Default is Low
                                 }
                             }
			 }
                        }
                        catch(UnsupportedMimeTypeException Type)
                        {
                          System.out.println("Unsupported Type...");//Link is Not a Website Like MP4, PDF, etc
                          synchronized(CrawlerDBM)
                          {
                          CrawlerDBM.DeleteURL(CurrentURL);//Remove This Link
                          }
                        }
                        catch(HttpStatusException Hex)
                        {
                          System.out.println("Page Not Found...");//Page Was Removed or Renamed
                          synchronized(CrawlerDBM)
                          {
                          CrawlerDBM.DeleteURL(CurrentURL);
                          } 
                        }
                        catch(UnknownHostException Disconnected)
                        {
                          System.out.println("Connection Failure...Rechecking in a Minute");//Page Was Removed or Renamed
                          synchronized(CrawlerDBM)
                          {
                          CrawlerDBM.SetVisited(CurrentURL, false);
                              try {
                                 
                                  this.sleep(60000);//Wait One Min
                                  
                              } 
                              catch (InterruptedException ex) 
                              {}
                          } 
                        }
           if(this.isInterrupted())//Check Interruption Flag 
           {
               System.out.println(this.getName()+" Terminated");//Terminate if Interrupted
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
            inStream=url.openStream();//Open URL
            InputStreamReader inStreamReader=new InputStreamReader(inStream);
            BufferedReader in=new BufferedReader(inStreamReader);
            String Arr[];
            
            str=in.readLine();
            
            if(str==null)//If End of File 
            {
                return;//Exit Function
            }  
                if(str.equals("User-agent: *"))//Find General User-Agent
                {
                    while(str!=null)//While Not The End Of File
                    {
                    if (str.contains("Disallow:"))//Search For Disallow
                    {
                        Arr=str.split(" ");
                        RURL=MyURL+Arr[1];//Create Absolute URL 
                        
                        synchronized(CrawlerDBM)
                        {
                        if(!CrawlerDBM.CheckRURL(RURL))//Is This Link In The DB
                        CrawlerDBM.AddRURL(RURL);//If Not Add It
                        }
                    }
                    str=in.readLine();
                    } 
                }
        } 
        catch(FileNotFoundException nf)//No Robot File
        {
            System.out.println("No Robot File");
        }
        catch(IOException IO)//Robot URL is Restricted
        {
            synchronized(CrawlerDBM)
            {
             if(!CrawlerDBM.CheckRURL(MyURL+"/robots.txt"))//Is This Link In The DB
            CrawlerDBM.AddRURL(MyURL+"/robots.txt");//If Not Add It
            }
        }
        catch (Exception ex) 
        {
            Logger.getLogger(Crawler.class.getName()).log(Level.SEVERE, null, ex);
        }   
        }
        
        public void DownloadHTML(Document MyDoc,int ID) throws IOException//Download The HTML Document Of A Link. Using It's DB ID As A File Name
        {
           File MyFile=new File(ID+".html");//Create A New File
           FileUtils.writeStringToFile(MyFile, MyDoc.outerHtml(),"UTF-8");//Write HTML Document in The File
        }
}
    

