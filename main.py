from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
import writeFile as wf
ls_months=['enero','febrero','marzo','abril','mayo','junio','julio','agosto','septiembre','octubre','noviembre','diciembre']

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
    querySt="select book_number,dt_publication_date,publication_date,id_thesis from thesis.tbthesis where period_number="+str(period)+""
    statement = SimpleStatement(querySt, fetch_size=1000)
    for row in session.execute(statement):
        id_thesis=''
        book_number=''
        date=''
        #subject=''
        wf.appendInfoToFile(pathToHereWin,str(period)+'_bookNumber.txt',row[0]+'  '+str(row[1])+'  '+row[2])
        id_thesis=str(row[3])
        
        #Case: Book number 2nd position starts with month
        """
        book_number=str(row[0])
        chunks=''
        chunks=book_number.split(',')
        if len(chunks)==3:
            val=''
            val=chunks[2].strip()
            if val!='':
                if val.find(' ')!=-1: 
                    valChunks=val.split(' ')
                    month=''
                    month=valChunks[0]
                    if month.find('-')!=-1:
                        dashChunk=month.split('-')
                        month=dashChunk[1].lower()  
                        for item in ls_months:
                            if month==item:
                                date=getCompleteDate(month+' de '+valChunks[2])
                                updateSt="update thesis.tbthesis set publication_date='"+str(val)+"', dt_publication_date='"+date+"' where id_thesis="+id_thesis
                                print('ID:',id_thesis)              
                                future = session.execute_async(updateSt)
                                res= future.result() 
                                break
                         
        
        """
        
        """
        if date!=date_null:
            book_number=row[0] 
            #chunks=''
                
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
        """  
                                
    cluster.shutdown()  
    
    
def getCompleteDate(pub_date):
    pub_date=pub_date.strip()
    if pub_date!='':
        if pub_date.find(' ')!=-1:
            # Day month year and hour
            chunks=pub_date.split(' ')
            #day=str(chunks[1].strip())
            month=str(chunks[0].strip())
            year=str(chunks[2].strip()) 
        elif pub_date.find(':')!=-1:
            chunks=pub_date.split(':')
            date_chunk=str(chunks[1].strip())
            data=date_chunk.split(' ')
            month=str(data[3].strip())
            day=str(data[1].strip())
            year=str(data[5].strip())
        month_lower=month.lower()
        for item in ls_months:
            if month_lower==item:
                month=str(ls_months.index(item)+1)
                if len(month)==1:
                    month='0'+month
                    break
        if month=='06':
            day='30'
        if month=='12':
            day='31'                
    completeDate=year+'-'+month+'-'+day                   
    return completeDate      
          

class CassandraConnection():
    cc_user='quartadmin'
    cc_keyspace='thesis'
    cc_pwd='P@ssw0rd33'
    cc_databaseID='9de16523-0e36-4ff0-b388-44e8d0b1581f'
    
    
if __name__=='__main__':
    main()    
    