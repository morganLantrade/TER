from sortMerge import *
from cartesian import *
from tools import *
from hash import *
from index import *
import time

#Oral Mercredi 30 aout 13h30

if __name__ == '__main__':
    
    Rsize=1500
    Ssize=10000
    selectivity=1
    memory=7
    pageSize=32
    folderName="Run2"

    ram=dict()
    stat=dict()
    free_space=10
    level=3
    root=50
    passe=1
    
    cartesian_product_index_file(folderName,memory,pageSize)
    #cartesian_product_index(folderName,selectivity,memory,pageSize)
    #cartesian_product_file(folderName,memory,pageSize)
    #s,_,_,_=search_index(folderName,ram,free_space,level,704,stat,root)
    #print(s)
    

    
    #Generation de données

    #R,S=generate_db(Rsize,Ssize,selectivity,double=False)
    #db_to_file(R,pageSize,folderName,"R")
    #db_to_file(S,pageSize,folderName,"S")
    #index_to_file2("Run1","R",memory,pageSize)
    #index_to_file("Run1",pageSize,"R")
    #lvl=index_to_file(folderName,pageSize,"S")
    #root=read_X_pages(folderName+"_idx2/I",486,1)
    #
    #
    #seconds=time.time()
    #

    #--------Test--------

    #Théorique

    #print(cartesian_product(folderName,selectivity,memory,pageSize))
    #print(sort_merge_join(folderName,selectivity,memory,pageSize))

    #Pratique

    #cartesian_product_file(folderName,memory,pageSize)
    #sort_merge_file(folderName,memory,pageSize)
    #simple_hash_join_file(folderName,memory,pageSize)

    #90,1,205

    #print("Done in : "+ str(round(time.time()-seconds,2))+"s")
    #time.sleep(5)




    #test_cartesian_product(Rsize=1000,Ssize=2000,selectivity=0.25,memory=3,size_of_tuple=32,size_of_page=1024)
    #test_cartesian_product_index(Rsize,Ssize,selectivity,memory,size_of_tuple,size_of_page,size_key_index)
    #test_sort_merge_join(Rsize,Ssize,selectivity,memory,size_of_tuple,size_of_page)
    #test_hybrid_hash_join(Rsize,Ssize,selectivity,memory,size_of_tuple,size_of_page)
    
    
  