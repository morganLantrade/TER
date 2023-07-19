from sortMerge import *
from cartesian import *
from tools import *
from hash import *
from index import *

#Oral Mercredi 30 aout 13h30

if __name__ == '__main__':
    
    Rsize=1000*32-5
    Ssize=Rsize*2
    selectivity=1
    memory=110
    pageSize=32
    folderName="R101S202Sel1"
    LMemory=[13,60,110]

    
    

    
    #Generation de données

    #R,S=generate_db(Rsize,Ssize,selectivity,double=False)
    #db_to_file(R,pageSize,folderName,"R")
    #db_to_file(S,pageSize,folderName,"S")

    db=pd.DataFrame()
    db2=read_X_pages(folderName+"/R",1,1) 

    #--------Test--------
    H={0:0}
    #
    seconds=time.time()
    #


    #time_test(simple_hash_join_file,folderName,pageSize,LMemory)


    #Théorique

    #print(cartesian_product(folderName,selectivity,memory,pageSize))
    #print(sort_merge_join(folderName,selectivity,memory,pageSize))

    #Pratique

    #cartesian_product_file(folderName,memory,pageSize)
    #sort_merge_file(folderName,memory,pageSize) #a modif
    #simple_hash_join_file(folderName,memory,pageSize)
    #grace_hash_join_file(folderName,memory,pageSize) #a modif
    #hybrid_hash_join_file(folderName,memory,pageSize)
    #L=[]

    for i in range(1000000):
        H[0]
        
        '''with open("Data/"+folderName+"/R_1.csv", newline='') as csvfile:

            spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
            a=next(spamreader)
            for x,y in spamreader:
                L.append((int(x),int(y)))
    db=pd.DataFrame(L,columns=a)'''
    
    finalTime=time.time()-seconds
    print("Done in : "+ str(finalTime)+"s")
    
    #time.sleep(5)
    print((finalTime*1000)/1000000)




    #test_cartesian_product(Rsize=1000,Ssize=2000,selectivity=0.25,memory=3,size_of_tuple=32,size_of_page=1024)
    #test_cartesian_product_index(Rsize,Ssize,selectivity,memory,size_of_tuple,size_of_page,size_key_index)
    #test_sort_merge_join(Rsize,Ssize,selectivity,memory,size_of_tuple,size_of_page)
    #test_hybrid_hash_join(Rsize,Ssize,selectivity,memory,size_of_tuple,size_of_page)
    
    