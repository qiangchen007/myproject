#!/usr/bin/python
#coding=utf-8
'''
Created on 2016-12-01
@author:chenqiang
'''
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import MySQLdb
import cx_Oracle
from MySQLdb.constants import FIELD_TYPE
from optparse import OptionParser
import time
import datetime
import os
import MySQLdb.cursors
# from warnings import filterwarnings
# filterwarnings('error', category = MySQLdb.Warning)
import threading
import Queue

cnt_err=0
# cnt_warn=0

def get_cli_options():
    parser = OptionParser(usage="usage: python %prog [options]",
                          description=""" DATA migrate  Oracle to  MySQL""")

    parser.add_option("-O", "--from_tns",
                      dest="orastring",
                      default="null",
                      metavar="orastrs",
                      help="oracle connect str")

    parser.add_option("-L", "--to_host",
                      dest="host",
                      default="local2",
                      metavar="host:port",
                      help="mysql host port")

    parser.add_option("-T", "--from_table",
                      dest="tab1",
                      default="slprod.sl$actor",
                      metavar="tab1",
                      help="source table")

    parser.add_option("-S", "--to_table",
                      dest="tab2",
                      default="dual",
                      metavar="tab2",
                      help="dest table")
    (options, args) = parser.parse_args()

    return options

def ora_client(orastring):
    try:
        os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.AL32UTF8'
        oracon = cx_Oracle.connect(user='logminer', password='logIner2017', dsn=orastring,threaded=True)
    except  cx_Oracle.Error,e:
        print "Error %s" %(e.args[0])
        exit(1)
    return oracon

def to_client(hostport):
    try:
        host=hostport.strip().split(':')[0]
        port=hostport.strip().split(':')[1]
        conn = MySQLdb.connect(host=host,port=int(port),user='bizdata',\
                              passwd='BizdatA!1',db='dajob_bizdata',\
                              charset='UTF8',use_unicode=True)
    except MySQLdb.Error, e:
        print "Error connecting %d: %s" % (e.args[0], e.args[1])
    return conn

def pkCheck(tab1):
    options = get_cli_options()    
    cr=ora_client(options.orastring).cursor()
    pkcols=[]
    TAB1=tab1.upper()
    sql_pk="select pkcol from PK_LIST where TABLE_OWNER='SLPROD' and TABLE_NAME= '%s' " %TAB1
    cr.execute(sql_pk)
    while (1): 
        row = cr.fetchone()
        if row==None:break
        pkcols.append(row[0])
    if pkcols:
        sql_col_type="select DATA_TYPE from DBA_TAB_COLUMNS where OWNER = 'SLPROD' and TABLE_NAME = '%s' and COLUMN_NAME = '%s' "  %(TAB1,pkcols[0]) 
        cr.execute(sql_col_type)
        coltype=cr.fetchone()[0]
        cr.close()
        if coltype == 'NUMBER' :
            return pkcols[0]
        elif coltype != 'NUMBER' : 
            print "ERROR: the PK type of the table %s is not NUMBER,exit ... " %TAB1
            exit(1)
    else:
        print "ERROR: the table %s has no PK,exit ... " %TAB1
        cr.close()
        exit(1)        
    
def queuein_PK(tab1,pk0,step,queue):
    options = get_cli_options()
    cr1=ora_client(options.orastring).cursor()
    sql_PKmax="select max(%s) from SLPROD.%s " %(pk0,tab1)
    cr1.execute(sql_PKmax)
    pk_max=cr1.fetchone()[0]
    cr1.close()
    pk_low=0
    pk_high=0
    cr2=ora_client(options.orastring).cursor()  
    while (pk_high<pk_max) :  
        sql_PKhigh="select max(%s) from \
                        (select %s from (select %s from SLPROD.%s where %s > %d  order by %s ) \
                            where ROWNUM <= %d ) " %(pk0,pk0,pk0,tab1,pk0,pk_low,pk0,step)
        cr2.execute(sql_PKhigh)
        pk_high=cr2.fetchone()[0]
        queue.put((pk_low,pk_high))
        pk_low=pk_high
    cr2.close() 

def rawcols_list():
    rawCols=[]
    options = get_cli_options()
    cr=ora_client(options.orastring).cursor()
    rawSql="select distinct(COLUMN_NAME) from RAW_LIST "
    cr.execute(rawSql)
    while (1): 
        row = cr.fetchone()
        if row==None:break
        rawCols.append(row[0])
    cr.close()
    return rawCols

def column_list(tab1):
    fields=[]
    comma=','
    options = get_cli_options()
    cr=ora_client(options.orastring).cursor()
    sql_tab1="select * from SLPROD.%s where 1=2 " %(tab1)
    cr.execute(sql_tab1)
    for desc in cr.description :
        if desc[0] not in rawcols_list():
            fields.append(desc[0])
    cr.close()
    return comma.join(fields)

def export_data(thread_seq,tab1,pk0,queue,tab2):
    global cnt_err
    global cnt_warn
    columns=column_list(tab1)
    while True:
        if queue.empty():break
        (low,high)=queue.get()
        options = get_cli_options()
        orasor=ora_client(options.orastring).cursor()
        forsql="select %s from SLPROD.%s where %s > %d and %s <= %d order by %s " %(columns,tab1,pk0,low,pk0,high,pk0) 
        mysor=to_client(options.host).cursor()
        tosql ="insert into %s(%s) values" %(tab2,columns) 
        counter=0
        orasor.execute(forsql)
        num_fields = len(orasor.description)
        try :
            while True:
                counter=counter+1
                row=""
                row1 = orasor.fetchone()
                if row1 == None:break
                for i in range(0,num_fields):
                    if type(row1[i]) is int :
                        row +=','+str(row1[i])
                    elif type(row1[i]) is float:
                        row +=','+repr(row1[i])
                    elif row1[i] is None:
                        row +=','+'NULL'
                    else:
                        row +=','+str('"'+str(row1[i])+'"')

                sql2= tosql+"("+ row[1:]+")"
                try:
                    mysor.execute(sql2)
#                except MySQLdb.Warning,w:
#                    print "Warning: %s" % str(w)
#                    print "Warning happens on the row: col0=%d ,PK range of the Batch is from %d to %d,ignore and continue... " %(row1[0],low,high)
#                    cnt_warn+=1
#                    continue
                except MySQLdb.Error,e:
                    print "Error %d: %s" % (e.args[0],e.args[1])
                    print "Error happens on the row: col0=%d ,PK range of the Batch is from %d to %d,ignore and continue... " %(row1[0],low,high)
                    cnt_err+=1
                    continue
                if counter%1000==0:
                    mysor.execute('commit')
#                    print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))," Thread %d commit: %d record. " %(thread_seq,counter)
                elif row1[0]==high:
                    mysor.execute('commit')
                    print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))," Thread %d commit last %d record. " %(thread_seq,counter)
        except (KeyboardInterrupt, SystemExit):
            print "Thread %d Exit ..." %(thread_seq)
            break
        finally:
            mysor.close()
            orasor.close()
            queue.task_done()

def main() :
    global cnt_err 
    options = get_cli_options()
    thread_list=[]
    thread_num=10
    SHARE_Q=Queue.Queue()
    rownum_step=100000
    pk_col0=pkCheck(options.tab1)

    print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))," Start ... "

    queuein_PK(options.tab1,pk_col0,rownum_step,SHARE_Q)
    for i in range(0,thread_num):
            thread_list.append(threading.Thread(target=export_data, args=(i,options.tab1,pk_col0,SHARE_Q,options.tab2)))
    for thread in thread_list:
        thread.start()
    for thread in thread_list:
        thread.join()
    SHARE_Q.join()

    print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))," Counter of err-row is %d .Finished ... " %(cnt_err)

if __name__ == '__main__':
    main()


