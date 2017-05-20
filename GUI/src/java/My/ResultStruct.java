package My;

public class ResultStruct 
{
    public String URL="";
    public float Rating=0;
    public int UID=0;
    
    public ResultStruct(int uid,String U,float ir)
    {
        URL=U;
        Rating=ir;  
        UID=uid;
    }  
}