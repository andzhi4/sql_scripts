CREATE OR REPLACE function ANDZHI4.load_data (
 p_table in varchar2,
 p_cnames in varchar2,
 p_dir in varchar2,
 p_filename in varchar2,
 p_delimeter in varchar2 default '|')
return number
is
l_input UTL_FILE.FILE_TYPE;
l_theCursor integer default DBMS_SQL.OPEN_CURSOR;
l_buffer varchar2(4000);
l_lastline varchar2(4000);
l_status integer;
l_col_cnt number default 0;
l_cnt number default 0;
l_sep char(1) default null;
l_errmsg varchar2(4000);

begin
execute immediate 'delete from badlog where auditdate>sysdate-30';
/*Open file with raw data*/
l_input := UTL_FILE.FOPEN(p_dir, p_filename, 'r', 4000);
/*Count commas in file*/
l_buffer := 'insert into ' || p_table || '(' || p_cnames || ') values (';
l_col_cnt := length(p_cnames)-length(replace(p_cnames, ',',''))+1;

for i in 1..l_col_cnt
loop
    l_buffer := l_buffer || l_sep || ':b'||i;
    l_sep := ',';
end loop;
l_buffer := l_buffer || ')';
/*insert into t(c1,c2...) values (:b1,:b2...))*/

dbms_sql.parse(l_theCursor, l_buffer, dbms_sql.native);

loop
    begin
        UTL_FILE.GET_LINE(l_input, l_lastline);
    exception
        when no_data_found then exit;
    end;
    
l_buffer := l_lastline || p_delimeter;

for i in 1..l_col_cnt
loop
    dbms_sql.bind_variable(l_thecursor, ':b'||i, substr(l_buffer, 1, instr(l_buffer, p_delimeter)-1));
    l_buffer := substr(l_buffer, instr(l_buffer, p_delimeter)+1);
end loop;

begin
    l_status := dbms_sql.execute(l_thecursor);
    l_cnt := l_cnt+1;
exception
    when others then 
        l_errmsg := sqlerrm;
        insert into badlog (errm, data, auditdate) values (l_errmsg, l_lastline, sysdate);
end;
end loop;

DBMS_SQL.CLOSE_CURSOR(l_thecursor);
utl_file.fclose(l_input);
commit;

return l_cnt;
exception
    when others then 
    dbms_sql.close_cursor(l_thecursor);
    if (utl_file.is_open(l_input)) then 
        utl_file.fclose(l_input);
    end if;
    raise;
end load_data;
/
