from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement

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
    querySt="select book_number,subject,dt_publication_date from thesis.tbthesis where period_number="+str(period)+""
    statement = SimpleStatement(querySt, fetch_size=1000)
    for row in session.execute(statement):
        book_number=''
        subject=''
        date=''
        book_number=row[0]  
        subject=row[1]
        date=row[2]
        if date!=date_null:
            print(date)
                      
        #future = session.execute_async(updateSt)
        #res= future.result();    
        #print(idThesis,': updated')
        #count=count+1
            
                              
    cluster.shutdown()          

class CassandraConnection():
    cc_user='quartadmin'
    cc_keyspace='thesis'
    cc_pwd='P@ssw0rd33'
    cc_databaseID='9de16523-0e36-4ff0-b388-44e8d0b1581f'
    
    
if __name__=='__main__':
    main()    
    