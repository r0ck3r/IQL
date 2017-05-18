package ru.webgrozny.iql.queryfilter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryFilter {
    private String sql;
    
    public QueryFilter(String SQL){
        this.sql = SQL;
    }
    
    public void setInt(int val){
        int pos = sql.indexOf('?');
        sql = sql.substring(0, pos) + val + sql.substring(pos + 1);
    }
    
    public void setString(String str){
        int pos = sql.indexOf('?');
        sql = sql.substring(0, pos) + '\'' + escapeString(str) + '\'' + sql.substring(pos + 1);
    }
    
    public void setBoolean(boolean boolVal){
        int pos = sql.indexOf('?');
        sql = sql.substring(0, pos) + (boolVal ? 1 : 0) + '\'' + sql.substring(pos + 1);
    }
    
    private String escapeString(String str){
        str = str.replace("\\","\\\\").replace("'", "\\'");
        Pattern pat = Pattern.compile("\\\\'");
        Matcher matcher = pat.matcher(str);
        int cnt = 0;
        while(matcher.find()){
            String found = matcher.group();
            if(found.length() % 2 > 0 ){
                str = str.substring(0, matcher.start() + cnt) + "\\" + str.substring(matcher.start() + cnt);
                cnt++;
            }
        }
        return str;
    }
    
    public String toString(){
        return sql;
    }
}
