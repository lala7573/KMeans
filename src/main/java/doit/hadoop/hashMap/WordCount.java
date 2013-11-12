package doit.hadoop.hashMap;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.*;


/**
 * Created with IntelliJ IDEA.
 * User: YeonjuMac
 * Date: 13. 9. 9.
 * Time: 오후 10:20
 * To change this template use File | Settings | File Templates.
 */
public class WordCount {
    private static HashMap<String, Integer> map;

    private static Set<Map.Entry<String, Integer>> set;
    private static Iterator<Map.Entry<String, Integer>> it;


    public static void main(String []args){
        System.out.println("hashmap.wordcount start:"+ args[0]);
        map = new HashMap<String, Integer>();


        try{
            FileRead(args[0]);
            print();

        }catch(Exception e){
            e.printStackTrace();
        }

    }

    public static void print(){

        set = map.entrySet();
        it = set.iterator();
        Map.Entry<String, Integer> e;
        System.out.println("asdf");

        while(it.hasNext()){
            System.out.println("asdf");
            e = (Map.Entry<String, Integer>)it.next();
            System.out.println("이름 : "+ e.getKey() + ", " + e.getValue());
        }
    }
    public static void map(String strLine){
        StringTokenizer tokenizer = new StringTokenizer(strLine, "\t\r\n\f|,.()<>'");
        while(tokenizer.hasMoreTokens()){
            String token = tokenizer.nextToken().toLowerCase();
            if( ! map.containsKey(token) )
                map.put(token, new Integer(1));
            else{
                map.put(token, new Integer(map.get(strLine).intValue() + 1)) ;         //한번에 리듀스까지함ㅋ..
            }

        }


    }

    public static void FileRead( String fileName) throws Exception{
        String strline = null;

        FileReader fr = new FileReader(fileName);
        BufferedReader br = new BufferedReader(fr);
        while((strline = br.readLine()) != null ) {
            map(strline);
        }
    }

    public static void FileWrite() throws Exception{
        String time = getCurrentTime();
        String fileName = time + ".txt";
        String output = null;

        File f = new File(fileName);
        FileOutputStream fs= new FileOutputStream(f);

        fs.close();
    }

    public static String getCurrentTime(){
        SimpleDateFormat date = new SimpleDateFormat("HH_mm_ss");
        Date dTime = new Date();
        String sTime = date.format(dTime);
        System.out.println("currentTime = " + sTime);

        return sTime;
    }

}
