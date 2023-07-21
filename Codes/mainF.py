from sortMerge import *
from cartesian import *
from tools import *
from hash import *
from index import *

#Oral Mercredi 30 aout 13h30

if __name__ == '__main__':
    
    Rsize=(10*32)-5
    Ssize=Rsize*2
    selectivity=1
    memory=13
    pageSize=32
    folderName="R101S202Sel1"
    LMemory=[13,60,110]

    
    

    
    #Generation de données

    R,S=generate_db(Rsize,Ssize,selectivity,double=False)
    db_to_file(R,pageSize,folderName,"R")
    db_to_file(S,pageSize,folderName,"S")

    #db=read_X_pages(folderName+"/R",1,1)
    #L=[]
    #--------Test--------
    
    #L.append((db["Y"].get(0),db["X"].get(0)))
    #print(type(L[0][0]))

    #sort_file(folderName,memory,pageSize,"R")

    #time_test(simple_hash_join_file,folderName,pageSize,LMemory)


    #Théorique

    #print(cartesian_product(folderName,selectivity,memory,pageSize))
    #print(sort_merge_join(folderName,selectivity,memory,pageSize))

    #Pratique


    #timer=cartesian_product_file(folderName,memory,pageSize)
    #timer=sort_merge_file(folderName,memory,pageSize) #a modif
    timer=simple_hash_join_file(folderName,memory,pageSize)
    #timer=grace_hash_join_file(folderName,memory,pageSize)
    #timer=hybrid_hash_join_file(folderName,memory,pageSize)

    print("Done in : "+ str(round(timer,2))+"s")
    
    #time.sleep(5)




    #test_cartesian_product(Rsize=1000,Ssize=2000,selectivity=0.25,memory=3,size_of_tuple=32,size_of_page=1024)
    #test_cartesian_product_index(Rsize,Ssize,selectivity,memory,size_of_tuple,size_of_page,size_key_index)
    #test_sort_merge_join(Rsize,Ssize,selectivity,memory,size_of_tuple,size_of_page)
    #test_hybrid_hash_join(Rsize,Ssize,selectivity,memory,size_of_tuple,size_of_page)
    
    