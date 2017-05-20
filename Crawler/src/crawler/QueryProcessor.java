package crawler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.tartarus.snowball.ext.EnglishStemmer;
//import org.apache.lucene.queryparser.

public class QueryProcessor 
{
    private String Query;
    private DatabaseManager QPDBM;
    private DatabaseManager RankerDBM=null;  
    private String RankerName;
    private int RankerNumber;
    public QueryProcessor(String Q,DatabaseManager DBM,int RID)
    {
        QPDBM=DBM;
        RankerDBM=DBM;
        Query = Q; 
        RankerNumber=RID;
        RankerName="R"+RID;
    }
    
    public ArrayList<ResultStruct> ProcessQuery(String Q) throws SQLException
    {
        SetQuery(Q);
        if(Phrase())
        {
         return ProcessPhrase();
        }
        else
        {
         return ProcessNonPhrase(ExtractWords(Query));    
        }
        
    }
    
    public void SetQuery(String Q)
    {
        Query=Q;
    }
    
    public boolean Phrase() throws SQLException
    {
        return (Query.endsWith("\"") && Query.startsWith("\""));
    }
    
    public ArrayList<ResultStruct> ProcessPhrase() throws SQLException
    {
        Query =Query.replaceAll("\"", "");
        String TempString=Query;
        String[] QWords=ExtractWords(TempString);
        float k=-1;
        float Max=-1;
        String MasterWord="";
        int MaxIndex=-1;
        if (QWords.length<1)
        {
         return null;
        }
        for(int i=0;i<QWords.length;i++)
        {
            k=QPDBM.GetWordIDF(QWords[i]);
            if (k==0) 
            {
                continue;
            }
            if(Max<k)
            {
                Max=k;
                MaxIndex=i;
                MasterWord=QWords[i];
            }
        }
        if(MaxIndex>-1 || Max>-1)
        {
        ArrayList<Integer> DocIDs=new ArrayList<Integer>();
        ArrayList<ResultStruct> RankedURLs=new ArrayList<ResultStruct>();
        ArrayList<PhraseStruct> PhraseDocRanks=new ArrayList<PhraseStruct>();
         
        DocIDs= QPDBM.GetRelevDocs(MasterWord);
        
        for(int i=0;i<DocIDs.size();i++)
        {
            String URLText=GetHTMLString(DocIDs.get(i));
            Document MyDoc=Jsoup.parse(URLText);
            Elements MyElements=MyDoc.getElementsContainingOwnText(Query);
            if(MyElements.isEmpty())
            {
                continue;
            }
            int PhraseFrequency=MyElements.size();
            PhraseStruct Temp=new PhraseStruct(DocIDs.get(i),PhraseFrequency);
            PhraseDocRanks.add(Temp);
        }
        int UID;
        PhraseDocRanks=SortPhraseDocs(PhraseDocRanks);
        for(int i=0;i<PhraseDocRanks.size();i++)
        {
            UID=PhraseDocRanks.get(i).UID;
            String URL=QPDBM.GetURLByID(UID);
            ResultStruct URTemp=new ResultStruct(UID, URL, PhraseDocRanks.get(i).Freq);
            RankedURLs.add(URTemp);
        }
        return RankedURLs;
        //return
        }  
        else
        {
            return null;
        }
    }
    
    public ArrayList<PhraseStruct> SortPhraseDocs(ArrayList<PhraseStruct> PSList)
    {   int j;
        int Max;
        int MaxID;
        if(PSList.size()<=1)
        {
            return PSList;
        }
        for(int i=0;i<PSList.size();i++)
        {
           Max=PSList.get(i).Freq;
           MaxID=i;
        for(j=i+1;j<PSList.size();j++)
        {
            if(Max<PSList.get(j).Freq)
            {
                Max=PSList.get(j).Freq;
                MaxID=j;
            } 
        }
            PhraseStruct PTemp= PSList.get(i);
            PSList.set(i, PSList.get(MaxID));
            PSList.set(MaxID, PTemp);
        }
        return PSList;
    }
    
    public String Stem(String S)//Breaking
    {      
    S=S.toLowerCase();
    EnglishStemmer stemmer = new EnglishStemmer();
     
    stemmer.setCurrent(S);
    stemmer.stem();
    return stemmer.getCurrent();
    }    
    
        public String[] ExtractWords(String TempS)
    {
       
       TempS=TempS.replace('é','e');
       TempS=TempS.replace('\'','\0');
       TempS=TempS.replaceAll("'", "");
       TempS=TempS.replaceAll("\n", ""); 
       TempS=TempS.replaceAll("\t", "");
       TempS=TempS.replaceAll("[^a-zA-Z\\s]", " ");
       
String[] MyWords=RemoveStopWords(TempS);
return MyWords;
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
        
    public ArrayList<ResultStruct> ProcessNonPhrase(String[] WordArray) throws SQLException
    {
        if(WordArray.length<1)
        {
            return null;
        }
        
        ArrayList<String> WordList=new ArrayList<String>();
        for(int i=0;i<WordArray.length;i++)
        {
            WordList.add(WordArray[i]);
        }
        float IDF;
        int OriginalSize=WordList.size();
        ArrayList<ResultStruct> List=new ArrayList<ResultStruct>();
        {
            boolean Created=false;
            for(int i=0;i<WordList.size();i++)
            {
                 IDF=RankerDBM.GetWordIDF(WordList.get(i));
               
                 if(i<OriginalSize)
                 {
                    String STWord=Stem(WordList.get(i));
                    ArrayList<URStruct> SimilarWords=RankerDBM.GetSimilarWords(STWord);
                    
                    for(int j=0;j<SimilarWords.size();j++)
                    {
                        WordList.add(SimilarWords.get(j).URL);
                    }
                    IDF=IDF*10;
                 }
                 if(IDF==0)
                {
                     System.out.println(WordList.get(i) +" Not Found in Database..");
                     continue;
                }

                 if(Created)
                 {
                     RankerDBM.InsertIntoTempTFIDFTable(RankerName,WordList.get(i), IDF);//You Must First Check That The Word Exists in The DB...
                 }
                 else
                 {
                     RankerDBM.CreateTempTFIDFTable(RankerName,WordList.get(i), IDF);
                     Created=true;
                 } 
            }
            if(Created)
            {
            List=RankerDBM.GetMultipleTFIDF(RankerNumber);
            RankerDBM.DropTempTable(RankerNumber);
            }
        }
        return List;
    } 
}
