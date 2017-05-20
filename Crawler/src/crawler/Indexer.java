package crawler;

import com.mysql.jdbc.exceptions.MySQLIntegrityConstraintViolationException;
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
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

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
           synchronized(IndexerDBM) 
           {
          try {
              MyURL = IndexerDBM.GetNotIndexedURL(URL_ID);
              
              if (MyURL.equals("")||URL_ID[0]==0)
              {
                  continue;
              }
              
              IndexerDBM.SetIndexed(MyURL, true);
          } 
          catch (SQLException ex) {
              Logger.getLogger(Indexer.class.getName()).log(Level.SEVERE, null, ex);
          }
           }

       //File HTML_File=new File(URL_ID[0]+".html");
       String HTML_Text=GetHTMLString(URL_ID[0]); 
String[] MyWords=ExtractWords(HTML_Text);

           try {
               CalculateFreq(MyWords,URL_ID);
           } catch (SQLException ex) {
               Logger.getLogger(Indexer.class.getName()).log(Level.SEVERE, null, ex);
           }
           
           finally{
           if(this.isInterrupted())
           {
               System.out.println(this.getName()+" Terminated");
               break;
           }
           }
       }
   }
    
    public String GetHTMLString(int HTML_ID)
    {
        //Convert the downloaded html file into a string
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
        //all stop words are saved in a file, open the file and read all of them and return
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
    String All_Text;
    Document MyDoc=Jsoup.parse(HTML_Text);
    All_Text=MyDoc.getElementsByTag("body").text();
    All_Text=All_Text.toLowerCase();
       
       All_Text=All_Text.replace('é','e');
       All_Text=All_Text.replace('\'','\0');
       All_Text=All_Text.replaceAll("'", "");
       All_Text=All_Text.replaceAll("\n", ""); 
       All_Text=All_Text.replaceAll("\t", "");
       All_Text=All_Text.replaceAll("[^a-zA-Z\\s]", " ");
       
String[] MyWords=RemoveStopWords(All_Text);
return MyWords;
    }
    
    public void CalculateFreq(String[] MyWords,int[] URL_ID) throws SQLException
    {
        //Calculate Frequency if each keyword and get all the positions of this keyword
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
        Position=FirstPos+1;
        MyList.add(Position); //Add the position of the word if it's found again
      if(!IndexerDBM.CheckKey(MyWords[i]))  //if keyword is not already in the database add it
      {
          IndexerDBM.AddKeyWord(MyWords[i]);
      }

       MyWS.word=MyWords[i];
       //System.out.println(this.getName()+" Current Word:" +MyWords[i]);
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
         KID=IndexerDBM.GetKeyID(MyWS.word);
         try{
//Indexer-Debuging 
//                     System.out.print(this.getName());
//                     System.out.print(" KID="+KID);
//                     System.out.print(" URLID="+URL_ID[0]);
//                     System.out.print(" Word="+MyWS.word);
//                     System.out.println(" Freq="+MyWS.Freq);
         IndexerDBM.AddKeyData(KID, URL_ID[0], MyWS.Freq);
            }
         catch(Exception CV)
                 {
                     System.out.println("MySQLIntegrityConstraintViolationException\n");
                     System.out.println("KID="+KID);
                     System.out.println("URLID="+URL_ID[0]);
                     System.out.println("Word="+MyWords[i]);
                     System.out.println("Word="+MyWS.word);
                 }
                      KDID=IndexerDBM.GetKeyDataID(KID, URL_ID[0]);
         if(KDID!=0)
         {
           for(int k=0;k<MyList.size();k++)
           {
             Position=(int)MyList.get(k);

             IndexerDBM.AddPosition(KDID, Position);
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
    
    
