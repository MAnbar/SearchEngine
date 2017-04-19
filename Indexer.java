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
       //File HTML_File=new File(URL_ID[0]+".html");
       String HTML_Text=GetHTMLString(URL_ID[0]); 
           try {
             
               synchronized(IndexerDBM){
               IndexerDBM.SetIndexed(MyURL, true);
               }
           }
            catch (SQLException ex) {
               Logger.getLogger(Indexer.class.getName()).log(Level.SEVERE, null, ex);
           }
           int[] Ranges = new int[10];
           String[] MyWords=ExtractWords(HTML_Text, Ranges);

           try {
               CalculateFreq(MyWords,URL_ID,Ranges);
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
           
    public String[] RemoveStopWords(String All_Text, int[] Ranges)
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
                   if(i<Ranges[0])
                   {
                       Ranges[0]--;
                   }
                   else if(Ranges[0] < i && i <Ranges[1])
                   {
                       Ranges[1]--;
                   }
                   else if(Ranges[1] < i && i <Ranges[2])
                   {
                       Ranges[2]--;
                   }
                   else if(Ranges[2] < i && i <Ranges[3])
                   {
                       Ranges[3]--;
                   }
                   else if(Ranges[3] < i && i <Ranges[4])
                   {
                       Ranges[4]--;
                   }
                   else if(Ranges[4] < i && i <Ranges[5])
                   {
                       Ranges[5]--;
                   }
                   else if(Ranges[5] < i && i <Ranges[6])
                   {
                       Ranges[6]--;
                   }
                   else if(Ranges[6] < i && i <Ranges[7])
                   {
                       Ranges[7]--;
                   }
                   else if(Ranges[7] < i && i <Ranges[8])
                   {
                       Ranges[8]--;
                   }
                   else
                   {
                       Ranges[9]--;
                   }
               }
            }
        } 
        return Words;
    }

    public int GetRange(int i,int[] Ranges)
    {
                   if(i<Ranges[0])
                   {
                       return 0;
                   }
                   else if(Ranges[0] < i && i <Ranges[1])
                   {
                       return 1;
                   }
                   else if(Ranges[1] < i && i <Ranges[2])
                   {
                       return 2;
                   }
                   else if(Ranges[2] < i && i <Ranges[3])
                   {
                       return 3;
                   }
                   else if(Ranges[3] < i && i <Ranges[4])
                   {
                       return 4;
                   }
                   else if(Ranges[4] < i && i <Ranges[5])
                   {
                       return 5;
                   }
                   else if(Ranges[5] < i && i <Ranges[6])
                   {
                       return 6;
                   }
                   else if(Ranges[6] < i && i <Ranges[7])
                   {
                       return 7;
                   }
                   else if(Ranges[7] < i && i <Ranges[8])
                   {
                       return 8;
                   }
                   else
                   {
                       return 9;
                   }
    }
    
    public String[] ExtractWords(String HTML_Text, int[] Ranges)
    {   
        String[] Temp;
        String All_Text="";
             Document MyDoc=Jsoup.parse(HTML_Text);
      // All_Text=MyDoc.getAllElements().text().toLowerCase();
    //Get All important text from the html text file (between certain tags)
    //Lower case all words
    //Remove stop words  
    All_Text =MyDoc.getElementsByTag("h1").text();
    Temp=All_Text.split(" ");
    Ranges[0]=Temp.length;
    All_Text+=MyDoc.getElementsByTag("h2").text();
    Temp=All_Text.split(" ");
    Ranges[1]=Temp.length-Ranges[0];
    All_Text+=MyDoc.getElementsByTag("h3").text();
    Temp=All_Text.split(" ");
    Ranges[2]=Temp.length-Ranges[1];
    All_Text+=MyDoc.getElementsByTag("h4").text();
    Temp=All_Text.split(" ");
    Ranges[3]=Temp.length-Ranges[2];
    All_Text+=MyDoc.getElementsByTag("h5").text();
    Temp=All_Text.split(" ");
    Ranges[4]=Temp.length-Ranges[3];
    All_Text+=MyDoc.getElementsByTag("h6").text();
    Temp=All_Text.split(" ");
    Ranges[5]=Temp.length-Ranges[4];
    All_Text+=MyDoc.getElementsByTag("b").text();
    Temp=All_Text.split(" ");
    Ranges[6]=Temp.length-Ranges[5];
    All_Text+=MyDoc.getElementsByTag("p").text();
    Temp=All_Text.split(" ");
    Ranges[7]=Temp.length-Ranges[6];
    All_Text+=MyDoc.getElementsByTag("li").text();//+ MyDoc.getElementsByTag("div").text();
    Temp=All_Text.split(" ");
    Ranges[8]=Temp.length-Ranges[7];
    All_Text+=MyDoc.getElementsByTag("PRE").text();//+ MyDoc.getElementsByTag("BLOCKQUOTE").text();
    Temp=All_Text.split(" ");
    Ranges[9]=Temp.length-Ranges[8];
   
    All_Text=All_Text.toLowerCase();

       All_Text=All_Text.replace('é','e');
       All_Text=All_Text.replace('\'','\0');
       All_Text=All_Text.replaceAll("[^a-zA-Z\\s]", " ");

 
String[] MyWords=RemoveStopWords(All_Text,Ranges);
return MyWords;
    }
    
    public void CalculateFreq(String[] MyWords,int[] URL_ID,int[] Ranges) throws SQLException
    {
        //Calculate Frequency if each keyword and get all the positions of this keyword
        WordStruct MyWS=new WordStruct("", 0);
        int KID,KDID;
        int Tag;
        int FirstPos=0;
        int Position=0;
        ArrayList MyList=new ArrayList();
        ArrayList TagList=new ArrayList();
   
for(int i=0;i<MyWords.length;i++)
{
       MyWS.Freq=0;
       
    if(!MyWords[i].equals(""))
    {        
        FirstPos++;
        Position=FirstPos;
        MyList.add(Position); //Add the position of the word if it's found again
        Tag=GetRange(i,Ranges);
        TagList.add(Tag);
        synchronized(IndexerDBM){
      if(!IndexerDBM.CheckKey(MyWords[i]))  //if keyword is not already in the database add it
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
               Tag=GetRange(j,Ranges);
               TagList.add(Tag);
               MyWS.Freq++;
               MyWords[j]="";
           }
       }
       synchronized(IndexerDBM)
       {
         KID=IndexerDBM.GetKeyID(MyWords[i]);
         IndexerDBM.AddKeyData(KID, URL_ID[0], MyWS.Freq);
       
                      KDID=IndexerDBM.GetKeyDataID(KID, URL_ID[0]);
         if(KDID!=0)
         {
           for(int k=0;k<MyList.size();k++)
           {
             Position=(int)MyList.get(k);
             Tag=(int)TagList.get(k);
             //IndexerDBM.AddPosition(KDID, Position);
             IndexerDBM.AddRanking(KDID, Position, Tag);
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
    
    
