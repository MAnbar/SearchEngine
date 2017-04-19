
package crawler;

public class QueryProcessor 
{
    private String Query;
    
    public QueryProcessor(String Q)
    {
        Query = Q; 
    }
    
    public String Stem(String S)//Breaking
    {
      S=S.split("er")[0];
      S=S.split("ing")[0];
      S=S.split("ed")[0];
      S=S.split("s")[0];
      
      return S;
    }
    
    
}
