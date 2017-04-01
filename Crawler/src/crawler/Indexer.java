package crawler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jsoup.Jsoup;
import java.util.ArrayList;
import java.util.List;
import org.jsoup.nodes.Document;

public class Indexer extends Thread
{
    private DatabaseManager IndexerDBM=null;
    
    public Indexer(DatabaseManager DB)
    {
        this.IndexerDBM=DB;
    }
    
    @Override
    public void run()
   {
       int[] URL_ID=new int[1];
       while(true)
       {
       
      String MyURL = null;
           try {
               synchronized(IndexerDBM)
               {
               MyURL = IndexerDBM.GetNotIndexedURL(URL_ID);
               }
               if (MyURL.equals(""))
               {
                   continue;
               }
           } catch (SQLException ex) {
               Logger.getLogger(Indexer.class.getName()).log(Level.SEVERE, null, ex);
           }
       if (URL_ID[0]==0)
       {
           continue;
       }
  File HTML_File=new File(URL_ID[0]+".html");
       String HTML_Text=GetHTMLString(URL_ID[0]); 
           try {
             
               synchronized(IndexerDBM){
               IndexerDBM.SetIndexed(MyURL, true);
               }
           }
            catch (SQLException ex) {
               Logger.getLogger(Indexer.class.getName()).log(Level.SEVERE, null, ex);
           }

String[] MyWords=ExtractWords(HTML_Text);

           try {
               CalculateFreq(MyWords,URL_ID);
           } catch (SQLException ex) {
               Logger.getLogger(Indexer.class.getName()).log(Level.SEVERE, null, ex);
           }
           
           if(this.isInterrupted())
           {
               System.out.println(this.getName()+" Terminated");
               break;
           }
       }
   }
    
    public String GetHTMLString(int HTML_ID)
    {
        		try {
			File file = new File(HTML_ID+".html");
			FileReader fileReader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			StringBuffer stringBuffer = new StringBuffer();
			String line;
			while ((line = bufferedReader.readLine()) != null) {
				stringBuffer.append(line);
				stringBuffer.append("\n");
			}
			fileReader.close();
			//System.out.println("Contents of file:");
			//System.out.println(stringBuffer.toString());//For Testing
                        return stringBuffer.toString();
                        //File not found
		} catch (IOException e) {
			e.printStackTrace();
                        return "";
		} 
                        
    }
    public String[] GetStopWords()
    {        
        		try {
			File file = new File("stop wordlist.txt");
			FileReader fileReader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			StringBuffer stringBuffer = new StringBuffer();
			String line;
			while ((line = bufferedReader.readLine()) != null) {
				stringBuffer.append(line);
				stringBuffer.append("\n");
			}
			fileReader.close();
			//System.out.println("Contents of file:");
			//System.out.println(stringBuffer.toString());//For Testing
                        return stringBuffer.toString().split("\n");
                        //File not found
		} catch (IOException e) {
			e.printStackTrace();
                        return null;
		}               
    }
           
    public String[] RemoveStopWords(String All_Text)
    {
        String[] StopWords=GetStopWords();   
        String[] Words=All_Text.split(" ");
        
        for(int i=0;i<Words.length;i++)
        {
            //Words[i].replace("[^a-zA-Z0-9\\s]", " ");
            for(int j=0;j<StopWords.length;j++)
            {
               if(Words[i].equals(StopWords[j]))
               {
                   Words[i]="";
               }
            }
        } 
        return Words;
    }

    public String[] ExtractWords(String HTML_Text)
    {
        String All_Text="";
             Document MyDoc=Jsoup.parse(HTML_Text);
      // All_Text=MyDoc.getAllElements().text().toLowerCase();
      
    All_Text=MyDoc.getElementsByTag("h1").text()+ "\n\n"+MyDoc.getElementsByTag("h2").text();
    All_Text+=MyDoc.getElementsByTag("h3").text()+ "\n\n"+ MyDoc.getElementsByTag("h4").text();
    All_Text+=MyDoc.getElementsByTag("h5").text()+ "\n\n"+ MyDoc.getElementsByTag("h6").text();
    All_Text+=MyDoc.getElementsByTag("b").text()+ "\n\n"+ MyDoc.getElementsByTag("p").text();
    All_Text+="\n\n"+MyDoc.getElementsByTag("li").text();//+ MyDoc.getElementsByTag("div").text();
    All_Text+="\n\n"+MyDoc.getElementsByTag("PRE").text();//+ MyDoc.getElementsByTag("BLOCKQUOTE").text();
    
    All_Text=All_Text.toLowerCase();

       All_Text=All_Text.replace('é','e');
       All_Text=All_Text.replace('\'','\0');
       All_Text=All_Text.replaceAll("[^a-zA-Z\\s]", " ");
       /*
       All_Text=All_Text.replace('.',' ');
       All_Text=All_Text.replace(':',' ');
       All_Text=All_Text.replace(';',' ');
       All_Text=All_Text.replace(',',' ');
       All_Text=All_Text.replace('/',' ');
       All_Text=All_Text.replace(')',' ');
       All_Text=All_Text.replace('(',' ');
       All_Text=All_Text.replace(']',' ');
       All_Text=All_Text.replace('[',' ');
       All_Text=All_Text.replace('?',' ');
       All_Text=All_Text.replace('!',' ');
       All_Text=All_Text.replace('"',' ');
       All_Text=All_Text.replace('\'',' ');
       All_Text=All_Text.replace('™',' ');
       All_Text=All_Text.replace('®',' ');
       All_Text=All_Text.replace('@',' ');
       All_Text=All_Text.replace('#',' ');
       All_Text=All_Text.replace('©',' ');
 */
 
String[] MyWords=RemoveStopWords(All_Text);
return MyWords;
    }
    
    public void CalculateFreq(String[] MyWords,int[] URL_ID) throws SQLException
    {
        WordStruct MyWS=new WordStruct("", 0);
        int KID,KDID;
        int FirstPos=0;
        int Position=0;
        ArrayList MyList=new ArrayList();
   
for(int i=0;i<MyWords.length;i++)
{
       MyWS.Freq=0;
       
    if(!MyWords[i].equals(""))
    {        
        FirstPos++;
        Position=FirstPos;
        MyList.add(Position);
        synchronized(IndexerDBM){
      if(!IndexerDBM.CheckKey(MyWords[i]))
      {
          IndexerDBM.AddKeyWord(MyWords[i]);
      }
        }
        
       MyWS.word=MyWords[i];
       //System.out.println("Current Word:" +MyWords[i]);
       MyWS.Freq=1;
       for(int j=i+1;j<MyWords.length;j++)
       {
            if(!MyWords[j].equals(""))
            {
                Position++;
            }
           if(MyWords[i].equals(MyWords[j]))
           {
               MyList.add(Position);
               MyWS.Freq++;
               MyWords[j]="";
           }
       }
       synchronized(IndexerDBM)
       {
         KID=IndexerDBM.GetKeyID(MyWords[i]);
         IndexerDBM.AddKeyData(KID, URL_ID[0], MyWS.Freq);
       
         for(int k=0;k<MyList.size();k++)
         {
             Position=(int)MyList.get(k);
             KDID=IndexerDBM.GetKeyDataID(KID, URL_ID[0]);
             if(KDID!=0)
             {
             IndexerDBM.AddPosition(KDID, Position);
             }
         }
       }
        MyList.clear();
    }
    else
    {
        continue;
    }
}
   
    }
}
    
    
