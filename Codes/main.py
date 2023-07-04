from sortMerge import *
from cartesian import *
from tools import *
from hash import *
import time

if __name__ == '__main__':
    
    Rsize=300
    Ssize=100000
    selectivity=1
    memory=3
    pageSize=32
    size_of_page=1024
    size_of_tuple=32
    size_key_index=8
    folderName="R1000S10000Sel1"


    #Generation de données


    #R,S=generate_db(Rsize,Ssize,selectivity,double=False)
    #db_to_file(R,pageSize,folderName,"R")
    #db_to_file(S,pageSize,folderName,"S")
    seconds=time.time()


    '''--------Test--------'''

    #Théorique

    #print(cartesian_product(folderName,selectivity,memory,pageSize))
    #print(sort_merge_join(folderName,selectivity,memory,pageSize))

    #Pratique

    #cartesian_product_file(folderName,memory,pageSize)
    sort_merge_file(folderName,memory,pageSize)



    print("Done in : "+ str(round(time.time()-seconds,2))+"s")
    #time.sleep(5)




    #test_cartesian_product(Rsize=1000,Ssize=2000,selectivity=0.25,memory=3,size_of_tuple=32,size_of_page=1024)
    #test_cartesian_product_index(Rsize,Ssize,selectivity,memory,size_of_tuple,size_of_page,size_key_index)
    #test_sort_merge_join(Rsize,Ssize,selectivity,memory,size_of_tuple,size_of_page)
    #test_hybrid_hash_join(Rsize,Ssize,selectivity,memory,size_of_tuple,size_of_page)
    '''
    R,S=generate_db(Rsize,Ssize,selectivity,double=False)
    b,c=number_of_pages(R,32,1024)
    print("Taille de R:")
    print("Nombre de pages : ",b)
    print("Index : ",c)

    print("---------")
    b,c=number_of_pages(S,32,1024,"B-arbre",32)
    print("Taille de S:")
    print("Nombre de pages : ",b)
    print("Index : ",c)
    
    ##############################
    #sort-merge
    ##############################
    print("-"*10)
    print("Sort-merge")
    print("-"*10)
    
    A,r0,w0,r1,w1=sort_merge_join(R,S)
    print(A)
    print("----")
    print("Pre-Traitement:\n")
    print('Lecture :',r0,'/ Ecriture :',w0)
    print("----")
    print("Post-Traitement:\n")
    print('Lecture :',r1,'/ Ecriture :',w1)
    print("--")
    print('Total : Lecture : ',r0+r1,' / Ecriture : ',w0+w1)
    ##############################
    #cartesien
    ##############################
    print("-"*10)
    print("Cartesian")
    print("-"*10)
   
    A,r0,w0,r1,w1=cartesian_product(R,S)
    print(A)
    print("----")
    print("Pre-Traitement:\n")
    print('Lecture :',0,'/ Ecriture :',0)
    print("----")
    print("Post-Traitement:\n")
    print('Lecture :',r1,'/ Ecriture :',w1)
    print("--")
    print('Total : Lecture : ',r0+r1,' / Ecriture : ',w0+w1)

    ##############################
    #hachage
    ##############################
    print("-"*10)
    print("Simple Hash")
    print("-"*10)

    A,r0,w0,r1,w1=hash_join(R,S)
    print(A)
    print("----")
    print("Pre-Traitement:\n")
    print('Lecture :',r0,'/ Ecriture :',w0)
    print("----")
    print("Post-Traitement:\n")
    print('Lecture :',r1,'/ Ecriture :',w1)
    print("--")
    print('Total : Lecture : ',r0+r1,' / Ecriture : ',w0+w1)'''