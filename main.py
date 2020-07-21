from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
import writeFile as wf

pathToHereWin='C:\\Users\\Acer\\Documents\\quart\\appsquart\\cassandraProcessOnly\\'

def main():
    print('1. Set period')
    res=input()
    intPeriod=int(res)
    update(intPeriod)

def update(period):
    objCC=CassandraConnection()
    cloud_config= {
     'secure_connect_bundle': pathToHereWin+'secure-connect-dbquart.zip'
    }
    
    date_null='1000-01-01'
    auth_provider = PlainTextAuthProvider(objCC.cc_user,objCC.cc_pwd)
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()
    session.default_timeout=70
    print('Updating started...')
    querySt="select book_number,subject,dt_publication_date,id_thesis from thesis.tbthesis where period_number="+str(period)+""
    statement = SimpleStatement(querySt, fetch_size=1000)
    for row in session.execute(statement):
        id_thesis=''
        book_number=''
        subject=''
        date=''
        #wf.appendInfoToFile(pathToHereWin,'5ta.txt',row[0])
        id_thesis=str(row[3])
        date=row[2]
        #Case for date and book_number
        """
        if date!=date_null:
            book_number=row[0] 
            #chunks=''
        """        
        subject=row[1]
        chunks=''
        chunks=subject.split(',')
        if len(chunks)>1:
            subject1=''
            subject2=''
            subject3=''
            if len(chunks)==2:
                subject1=chunks[0]
                subject2=chunks[1]
                querySubject=" subject_1='"+str(subject1).strip()+"',subject_2='"+str(subject2).strip()+"',multiple_subjects=True"
                
            if len(chunks)==3:
                subject1=chunks[0]
                subject2=chunks[1]
                subject3=chunks[2]
                querySubject=" subject_1='"+str(subject1).strip()+"',subject_2='"+str(subject2).strip()+"',subject_3='"+str(subject3).strip()+"',multiple_subjects=True"
        else:
            #Subject is 1 only
            querySubject='multiple_subjects=False'        
    
        #Final update
        updateSt='update thesis.tbthesis set '+querySubject+ ' where id_thesis='+id_thesis
        print('ID:',id_thesis)
                      
        future = session.execute_async(updateSt)
        res= future.result()    
        
            
                              
    cluster.shutdown()          

class CassandraConnection():
    cc_user='quartadmin'
    cc_keyspace='thesis'
    cc_pwd='P@ssw0rd33'
    cc_databaseID='9de16523-0e36-4ff0-b388-44e8d0b1581f'
    
    
if __name__=='__main__':
    main()    
    