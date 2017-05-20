package My;

import java.beans.PropertyVetoException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.jsoup.nodes.Element;
import org.tartarus.snowball.ext.EnglishStemmer;




public class MatrixForm extends HttpServlet {
    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException, PropertyVetoException, SQLException {
        response.setContentType("text/html;charset=UTF-8");
        try (PrintWriter out = response.getWriter()) {
            
            String UQuery=request.getParameter("UserQuery");
            DatabaseManager UIDB=new DatabaseManager();
            QueryProcessor QP=new QueryProcessor(UQuery, UIDB, 2);
           ArrayList<ResultStruct> Results=new ArrayList<ResultStruct>();
           if(UQuery!=null)
           {
           Results=QP.ProcessQuery(UQuery);
           }

            
            
            out.println("<!DOCTYPE html>");
            out.println("<html>");
            out.println("<head>");
            out.println("<title>Search Results For "+UQuery+"</title>");            
            out.println("</head>");
            out.println("<body bgcolor=\"#f0f0f0\" align=\"left\">");
            out.println("<h1> Showing Results For: "+UQuery+ " </h1></br>");
            out.println("<input name=\"UserQuery\" form=\"MatrixForm\" placeholder=\" \"></input>");
            out.println("<form action=\"MatrixForm\" method=\"GET\" id=\"MatrixForm\">");
	    out.println("<input type=\"submit\" value=\"Search\" />");
	    out.println("</form>");
            
                   if(!Results.isEmpty())
            {
        for(int i=0;i<Results.size();i++)
        {
            out.print("<h6> "+"---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- "+(i+1)+" </h6>");
            out.print("<h2>");
            out.print("<a href="+Results.get(i).URL+" title="+Results.get(i).URL+" </a>"+Results.get(i).URL+"</a>");
            out.println("</h2>");
            out.println("<h3> "+GetSnippet(UQuery, Results.get(i).UID)+"</h3></br>");
        }     
            }
                   else
                   {
                    out.println("</br><h1> No Results </h1></br>");

                   }
            out.println("</body>");
            out.println("</html>");
        }
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        try {
            processRequest(request, response);
        } catch (PropertyVetoException ex) {
            Logger.getLogger(MatrixForm.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(MatrixForm.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        try {
            processRequest(request, response);
        } catch (PropertyVetoException ex) {
            Logger.getLogger(MatrixForm.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(MatrixForm.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
   
    public String GetSnippet(String Query,int DocNumber)
    {
        Query=Query.toLowerCase();
        if((Query.endsWith("\"") && Query.startsWith("\"")))
        {
        Query=Query.substring(1,Query.length()-1);
        }
        else
        {
            Query=Stem(Query);
        }
        String URLText=GetHTMLString(DocNumber);
            Document MyDoc=Jsoup.parse(URLText);
            //Element MyElement=MyDoc.getElementsContainingOwnText(Query).first();
            Elements MyElements=MyDoc.getElementsByTag("p");
            Element MyElement=null;
            for(int i=1;i<MyElements.size();i++)
            {
                if(MyElements.get(i).getElementsContainingText(Query).first()!=null)
                {
                    MyElement=MyElements.get(i).getElementsContainingOwnText(Query).first();
                    break;
                }  
            }
            if(MyElement==null||MyElement.text().isEmpty())
            {
                MyElement=MyDoc.getElementsContainingText(Query).first();
            }
            if(MyElement==null||MyElement.text().isEmpty())
            {
                MyElement=MyDoc.getElementsByTag("body").first();
            }
//            int Counter=0;
//            while(MyElement==null||MyElement.text().isEmpty())
//            {
//                if(Counter<MyElements.size())
//                {
//                MyElement=MyElements.get(Counter);
//                }
//                else
//                {
//                  break;
//                }
//                Counter++;
//            }e
            String Snippet = MyElement.text();
            if(Snippet.length()<=200)
            {
            }
            else
            {               
                int End;

                int MyIndex = Snippet.indexOf(Query);
                if(MyIndex==-1)
                {
                    Snippet=Snippet.substring(0, 200)+"...";
                    return Snippet;
                }
                if(MyIndex<100)
                {
                    Snippet=Snippet.substring(0, 200)+"...";
                }
                else
                {
                    int start=MyIndex-99;
                    while(Snippet.charAt(start-1)!=' ')
                    {
                        if(start==0)
                        {
                            break;
                        }
                        start--; 
                    }
                    if(start+200<Snippet.length())
                    {
                        End=start+200;
                        Snippet=Snippet.substring(start, End)+"...";
                    }
                    else
                    {
                        End=Snippet.length();
                        Snippet=Snippet.substring(start, End);
                    }
                }
            }
            return Snippet;
    }

            public String GetHTMLString(int HTML_ID)
    {
        //Convert the downloaded html file into a string
        		try {
			File file = new File("C:\\Users\\Boori\\Documents\\NetBeansProjects\\Crawler_5.8\\"+HTML_ID+".html");
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
    public String Stem(String S)//Breaking
    {      
    S=S.toLowerCase();
    EnglishStemmer stemmer = new EnglishStemmer();
     
    stemmer.setCurrent(S);
    stemmer.stem();
    return stemmer.getCurrent();
    }        
    @Override
    public String getServletInfo() {
        return "Short description";
    }
}

