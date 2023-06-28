import random
import pandas as pd
import math
import os


def sort_file(folderName,memory,pageSize):
    assert memory>=3, "Erreur : La memoire doit contenir au moins 3 pages"

    nbPageR=len([f for f in os.listdir("Data/"+folderName) if "R_" in f])
    nbPageS=len([f for f in os.listdir("Data/"+folderName) if "S_" in f])

    if not os.path.exists('Data/'+folderName+"_sorted"):
        os.makedirs('Data/'+folderName+"_sorted")
 
    


    

def sort_merge(arr,i,memory,tuples_per_page):
    '''Fonction qui appelle la fonction recursive merge qui renvoie la liste arr triée 
    et le nombre de lecture et ecriture dans la memoire'''
    read_count = 0

    if len(arr) <= 1:
        return arr, read_count
    mid = len(arr) // 2
    left_half = arr[:mid]
    right_half = arr[mid:]

    left_half, left_reads, left_writes = sort_merge(left_half,i)
    right_half, right_reads, right_writes = sort_merge(right_half,i)

    read_count += left_reads + right_reads

    sorted_arr, merge_reads, merge_writes = merge(left_half, right_half,i)
    read_count += merge_reads

    return sorted_arr, math.ceil(len(arr)/tuples_per_page)


def merge(left, right,i):
    '''FOnction recursive qui effectue un tri fusion avec mémorisation des lectures et écritures dans la mémoire
    selon i l'index pour lequel le tuple est trié  , 1 pour R et 0 pour S'''
    merged = []
    left_index = 0
    right_index = 0
    reads = 0
    writes = 0

    while left_index < len(left) and right_index < len(right):
        if left[left_index][i] <= right[right_index][i]:
            merged.append(left[left_index])
            left_index += 1
        else:
            merged.append(right[right_index])
            right_index += 1
        writes += 1

    while left_index < len(left):
        merged.append(left[left_index])
        left_index += 1
        writes += 1

    while right_index < len(right):
        merged.append(right[right_index])
        right_index += 1
        writes += 1

    reads += len(left) + len(right)
    return merged, reads, writes

def db_to_file(db,pageSize,folderName,dbName):

    n=len(db.index)
    nb_of_files=n//pageSize
    if not os.path.exists('Data/'+folderName):
        os.makedirs('Data/'+folderName)
    for i in range(nb_of_files):
        db_temp=db.loc[i*pageSize:(i+1)*pageSize-1]
        db_temp.to_csv('Data/'+folderName+"/"+dbName+"_"+str(i+1)+".csv",sep=',',index=False)
    if (n%pageSize!=0):
        db_temp=db.loc[nb_of_files*pageSize:]
        db_temp.to_csv('Data/'+folderName+"/"+dbName+"_"+str(nb_of_files+1)+".csv",sep=',',index=False)


def generate_db(Rsize,Ssize,selectivity,double=False):
    '''Renvoi deux dataframe correspondant aux tables R(X,Y) et S(Y,Z)
    N : nombre d'élements dans chaque table
    selectivity : compris entre 0 et 1 , permet de déterminer le pourcentage
                  d'Y en communs de la table S et celle de R
    double : doublon possible ou non
    '''

    R=[]
    S=[]

    switch=False

    if Rsize>Ssize:
        Rsize,Ssize=Ssize,Rsize
        switch=True

    if double:
        RY=[random.randint(1,Rsize) for _ in range(Rsize)]
        uniqueRY=list(set(RY))
        
        relation=int(Rsize*selectivity) 

        #on récupere les Y en communs aux hasard
        SY=[random.sample(uniqueRY,1)[0] for _ in range(relation)]

        #on récupere des y inexistants dans R de facon aléatoire et uniforme
        L=[random.randint(-(Ssize//2),0) for _ in range((Ssize//2)+1)]
        L+=[random.randint(Rsize+1,Rsize+(Ssize//2)) for _ in range((Ssize//2)+1)]

        SY+=random.sample(L,Ssize-relation) 
        random.shuffle(SY)

    else:
        RY=[i for i in range(1,Rsize+1)]
        random.shuffle(RY)

        relation=math.ceil(Rsize*selectivity) 

        #on récupere les Y en communs aux hasard
        SY=random.sample(RY,relation)

        #on récupere des y inexistants dans R de facon aléatoire et uniforme

        L=[i for i in range(-Ssize,1)]
        L+=[i for i in range(Rsize+1,Rsize+Ssize+1)]

        SY+=random.sample(L,Ssize-relation) 
        random.shuffle(SY)

    if switch:
        RY,SY=SY,RY
        Rsize,Ssize=Ssize,Rsize

   
    #création des tables
    for i in range(Rsize):
        R.append((i+1,RY[i]))
    for i in range(Ssize):
        S.append((SY[i],i+1))
    R=pd.DataFrame(R,columns=['X','Y'])
    S=pd.DataFrame(S,columns=['Y','Z'])
    return R,S

def number_of_pages(dataframe,size_of_tuple,size_of_page,index="Not",size_key_index=8):
    '''Renvoie le nombre de pages de la table et la taille des différents niveau de l'index si index est B-arbre'''
    total_tuples=len(dataframe.index)
    Idx=dict()
    nb_tuples= size_of_page//size_of_tuple
    nb_pages= math.ceil(total_tuples/nb_tuples)
        
    if index=="B-arbre":
        i=0
        nb_tuples= size_of_page//(size_of_tuple//2+size_key_index) #on garde que l'attribut Y
        nb_pages_idx= math.ceil(total_tuples/nb_tuples)
        while nb_pages_idx>1:
            Idx[i]=nb_pages_idx
            nb_pages_idx= math.ceil(nb_pages_idx/nb_tuples)
            i+=1
        Idx[i]=1 #dernier niveau
    return nb_pages,Idx


def sort_merge_join(R,S,selectivity,memory,size_of_tuple,size_of_page):
    '''Renvoi un inner join des tables R et S en utilisant un algorithme de tri fusion'''

    n=len(R)
    m=len(S)

    ##Theoric##
    R_pages,_= number_of_pages(R,size_of_tuple,size_of_page)
    S_pages,_=number_of_pages(S,size_of_tuple,size_of_page)
    tuples_per_page= size_of_page//size_of_tuple
    
    #Build
    read_build_th = R_pages*(1+math.ceil(math.log(math.ceil(R_pages/memory),memory-1)))+S_pages*(1+math.ceil(math.log(math.ceil(S_pages/memory),memory-1)))
    written_build_th = read_build_th

    #Probe
    read_probe_th=R_pages+S_pages
    write_probe_th=math.ceil(math.ceil(len(R)*selectivity)/tuples_per_page)

    # A implementer
    read_build_exp=read_build_th
    written_build_exp = written_build_th

    # Tri des deux tables au préalable
    R,read1,written1=sort_merge(R.values.tolist(),1,memory,tuples_per_page)
    S,read2,written2=sort_merge(S.values.tolist(),0,memory,tuples_per_page)
    R=pd.DataFrame(R,columns=['X','Y'])
    S=pd.DataFrame(S,columns=['Y','Z'])
    T=[]
    iR=0
    iS=0
    written=0
    while iR<n and iS<m:
        if R['Y'].get(iR)==S['Y'].get(iS): 
            T.append((R['X'].get(iR),R['Y'].get(iR),S['Z'].get(iS)))
            written+=1
            iS+=1
            iR+=1
            
        elif R['Y'].get(iR)>S['Y'].get(iS):
            iS+=1
            
        else:
            iR+=1
    
    read_probe_exp=math.ceil(iR/tuples_per_page)+math.ceil(iS/tuples_per_page)
    written_probe_exp=math.ceil(written/tuples_per_page)

    T=pd.DataFrame(T,columns=['X','Y','Z'])
    return T,read_build_th,written_build_th,read_probe_th,write_probe_th,read_build_exp,written_build_exp,read_probe_exp,written_probe_exp

    #return ,read1+read2,written1+written2,read,written
    
def cartesian_product_file(folderName,memory,pageSize):
    assert memory>=3, "Erreur : La memoire doit contenir au moins 3 pages"

    nbPageR=len([f for f in os.listdir("Data/"+folderName) if "R_" in f])
    nbPageS=len([f for f in os.listdir("Data/"+folderName) if "S_" in f])

    b=memory-2 #taille bloc

    T=[]
    if not os.path.exists('Data/'+folderName+"_cp"):
        os.makedirs('Data/'+folderName+"_cp")
    nbPageT=0
    for i in range(nbPageR):
        R=pd.read_csv('Data/'+folderName+"/R_"+str(i+1)+".csv")
        for j in range (nbPageS):
            S=pd.read_csv('Data/'+folderName+"/S_"+str(j+1)+".csv")
            for k in range(len(R.index)):
                for l in range(len(S.index)):
                    if (R["Y"].get(k)==S["Y"].get(l)):
                        T.append((R['X'].get(k),R['Y'].get(k),S['Z'].get(l)))
                        if len(T)==pageSize:
                            pd.DataFrame(T,columns=['X','Y','Z']).to_csv('Data/'+folderName+"_cp/T_"+str(nbPageT+1)+".csv",sep=',',index=False)
                            nbPageT+=1
                            T=[]
    if T:
        pd.DataFrame(T,columns=['X','Y','Z']).to_csv('Data/'+folderName+"_cp/T_"+str(nbPageT+1)+".csv",sep=',',index=False)






def cartesian_product(R,S,selectivity,memory,size_of_tuple,size_of_page):
    '''Renvoi un  join des tables R et S en utilisant un algorithme de produit cartésien par block'''
    assert memory>=3, "Erreur : La memoire doit contenir au moins 3 pages"
    
    
    ##Theoric##
    R_pages,_= number_of_pages(R,size_of_tuple,size_of_page)
    S_pages,_= number_of_pages(S,size_of_tuple,size_of_page)
    tuples_per_page= size_of_page//size_of_tuple
    b=memory-2 #taille bloc
    th_read= R_pages  + S_pages*math.ceil(R_pages/b)
    th_written= math.ceil(math.ceil(len(R)*selectivity)/tuples_per_page)
    
        
    ##Experiment
    n=len(R)
    m=len(S)
    T=[]
    written=1 # dans tous les cas on ecrira sur au moins une page output
    read=0 

    for i in range(n):
        if i%tuples_per_page==0: #page suivante de R 
            read+=1 
        for j in range(m):
            if (i%(tuples_per_page*b)==0) and (j%tuples_per_page==0):  #bloc de b pages suivant donc on compte les pages de S lues
                read+=+1
            if R['Y'].get(i)==S['Y'].get(j):
                T.append((R['X'].get(i),R['Y'].get(i),S['Z'].get(j)))
                written+= int(len(T)%tuples_per_page==0)  #page suivante de T
                
    return pd.DataFrame(T,columns=['X','Y','Z']),th_read,th_written,read,written

def cartesian_product_index(R,S,selectivity,memory,size_of_tuple,size_of_page,size_key_index):
    '''Renvoi une simulation memoire d'un join des tables R et S en utilisant un algorithme de produit cartésien indexe sur S'''
    assert memory>=4, "Erreur : La memoire doit contenir au moins 4 pages"
    
    ##Theoric##
    R_pages,_= number_of_pages(R,size_of_tuple,size_of_page)
    S_pages,idx=number_of_pages(S,size_of_tuple,size_of_page,"B-arbre",size_key_index)
    tuples_per_page= size_of_page//size_of_tuple
    
    #Build
    read_build_th = S_pages 
    written_build_th = sum(idx.values()) #nombre pages index

    #Probe
    n=len(idx) # nombre de niveau a charger regulierement
    free_space=memory-2 
    read=0
    #tant qu'on peut stocker les niveaux de l'index dans la mémoire on réduit le nombre de niveau a charger
    while free_space-idx[(n-1)]>=1 and n>0:  
        free_space-= idx[(n-1)]
        read+=idx[(n-1)] # pages des niveaux que l'on charge qu'une seule fois
        n-=1
    
    
    
    read_probe_th = R_pages + len(R)*(n+1) + read  #niveau a charger + lecture effective de S sur le disque
    write_probe_th = written= math.ceil(math.ceil(len(R)*selectivity)/tuples_per_page)
    


    ##Experiment
    print("Pages par niveau de l'index :",idx)

    # A implementer un index B-arbre ou juste simulation theorique de l'index
    read_build_exp=read_build_th
    written_build_exp = written_build_th

    
    for i in range(len(R)):
        if i%tuples_per_page==0: #page suivante de R 
            read+=1 
        j=0
        while j<= n: #on ne charge que les niveaux a charger
            read +=1 
            j+=1 
    
    return read_build_th,written_build_th,read_probe_th,write_probe_th,read_build_exp,written_build_exp,read,written

def hash_join(R,S,selectivity,memory,size_of_tuple,size_of_page):
    ##Theoric##
    R_pages,_= number_of_pages(R,size_of_tuple,size_of_page)
    S_pages,_=number_of_pages(S,size_of_tuple,size_of_page)
    tuples_per_page= size_of_page//size_of_tuple

    #Build
    read_build_th = R_pages
    written_build_th = R_pages #nombre pages index
    
    #Probe
    read_probe_th = S_pages
    write_probe_th = math.ceil(math.ceil(len(R)*selectivity)/tuples_per_page)

    ##Experiment##

    #Build
    written=0
    read=0
    R=R.values.tolist()
    S=S.values.tolist()
    H=dict()
    T=[]
    for x,y in R:
        read+=1
        key=y%3
        written+=1
        if key in H:
            H[key].append((x,y))

        else:
            H[key]=[(x,y)]

    read_build_exp=math.ceil(read/tuples_per_page)
    written_build_exp=math.ceil(written/tuples_per_page)
    
    #Probe
    written=0
    read=0
    for y,z in S:
        read+=1
        key=y%3
        if key in H:
            for (Rx,Ry) in H[key]:
                if Ry==y:
                    T.append((Rx,Ry,z))
                    written+=1
    read_probe_exp=math.ceil(read/tuples_per_page)
    written_probe_exp=math.ceil(written/tuples_per_page)
    T=pd.DataFrame(T,columns=['X','Y','Z'])
    return T,read_build_th,written_build_th,read_probe_th,write_probe_th,read_build_exp,written_build_exp,read_probe_exp,written_probe_exp






def hybrid_hash_join(R,S,selectivity,memory,size_of_tuple,size_of_page):
    '''Renvoi une simulation memoire d'un join des tables R et S en utilisant un algorithme de hachage hybride'''
    
    ##Theoric##
    
    R_pages,_= number_of_pages(R,size_of_tuple,size_of_page)
    S_pages,_=number_of_pages(S,size_of_tuple,size_of_page)
    tuples_per_page= size_of_page//size_of_tuple
    pages_per_partition_R  = math.ceil(R_pages/(memory-1))
    print(R_pages,S_pages,memory)
    print("Page par partition de R", pages_per_partition_R)
    #page par partitions de la plus grande relation S + 1 page pour charger partition R + output
    assert memory>  pages_per_partition_R+2, f'Memory overflow : les partitions ont des taille de {pages_per_partition} pages pour une mémoire de {memory} pages dont deux sont requises pour le join'
    

    

    ##Experiment##
    if R_pages + 2 > memory: #on doit partitionner
        return None
    else:
        return hash_join(R,S,selectivity,memory,size_of_tuple,size_of_page)
        




def test_cartesian_product(Rsize,Ssize,selectivity,memory,size_of_tuple,size_of_page):
    R,S=generate_db(Rsize,Ssize,selectivity,double=False)
    T,th_read,th_written,exp_read,exp_written=cartesian_product(R,S,selectivity,memory,size_of_tuple,size_of_page)
    print("-"*10)
    print("Cartesian")
    print("-"*10)
    print("R")
    print(R)
    print("*"*5)
    print("S)")
    print(S)
    print("*"*5)
    print("T")
    print(T)
    print("----") 
    print("Entrée/Sortie theorique : \n")
    print('Lecture :',th_read,'/ Ecriture :',th_written)
    print("----")
    print("Entrée/Sortie experimentale: \n")
    print('Lecture :',exp_read,'/ Ecriture :',exp_written)
    print("--")

def test_cartesian_product_index(Rsize,Ssize,selectivity,memory,size_of_tuple,size_of_page,size_key_index):
    R,S=generate_db(Rsize,Ssize,selectivity,double=False)
    read_build_th,written_build_th,read_probe_th,write_probe_th,read_build_exp,written_build_exp,read,written=cartesian_product_index(R,S,selectivity,memory,size_of_tuple,size_of_page,size_key_index)
    print("-"*10)
    print("Cartesian")
    print("-"*10)
        
    print("----") 
    print("Theorique Build")
    print('Lecture :',read_build_th,'/ Ecriture :',written_build_th)
    print("----")
    print("Theorique Probe")
    print('Lecture :',read_probe_th,'/ Ecriture :',write_probe_th)
    print("----")
    print("Experimental Build: \n")
    print('Lecture :',read_build_exp,'/ Ecriture :',written_build_exp)
    print("----")
    print("Experimental Probe: \n")
    print('Lecture :',read,'/ Ecriture :',written)
    print("---")
    
def test_sort_merge_join(Rsize,Ssize,selectivity,memory,size_of_tuple,size_of_page):
    R,S=generate_db(Rsize,Ssize,selectivity,double=False)
    
    ##############################
    #sort-merge
    ##############################
    print("-"*10)
    print("Sort-merge")
    print("-"*10)
    
    T,read_build_th,written_build_th,read_probe_th,write_probe_th,read_build_exp,written_build_exp,read_probe_exp,written_probe_exp=sort_merge_join(R,S,selectivity,memory,size_of_tuple,size_of_page)
    print("R")
    print(R)
    print("*"*5)
    print("S)")
    print(S)
    print("*"*5)
    print("T")
    print(T)
    print("----") 
    print("Theorique Build")
    print('Lecture :',read_build_th,'/ Ecriture :',written_build_th)
    print("----")
    print("Theorique Probe")
    print('Lecture :',read_probe_th,'/ Ecriture :',write_probe_th)
    print("----")
    print("Experimental Build: \n")
    print('Lecture :',read_build_exp,'/ Ecriture :',written_build_exp)
    print("----")
    print("Experimental Probe: \n")
    print('Lecture :',read_probe_exp,'/ Ecriture :',written_probe_exp)
    print("---")

def test_hybrid_hash_join(Rsize,Ssize,selectivity,memory,size_of_tuple,size_of_page):
    R,S=generate_db(Rsize,Ssize,selectivity,double=False)
    print("-"*10)
    print("Hybrid Hash Join")
    print("-"*10)
    T,read_build_th,written_build_th,read_probe_th,write_probe_th,read_build_exp,written_build_exp,read_probe_exp,written_probe_exp=hybrid_hash_join(R,S,selectivity,memory,size_of_tuple,size_of_page)
    print(T)
    print("----") 
    print("Theorique Build")
    print('Lecture :',read_build_th,'/ Ecriture :',written_build_th)
    print("----")
    print("Theorique Probe")
    print('Lecture :',read_probe_th,'/ Ecriture :',write_probe_th)
    print("----")
    print("Experimental Build: \n")
    print('Lecture :',read_build_exp,'/ Ecriture :',written_build_exp)
    print("----")
    print("Experimental Probe: \n")
    print('Lecture :',read_probe_exp,'/ Ecriture :',written_probe_exp)
    print("---")

if __name__ == '__main__':
    
    Rsize=321
    Ssize=650
    selectivity=1
    memory=3
    pageSize=32
    size_of_page=1024
    size_of_tuple=32
    size_key_index=8
    #R,S=generate_db(Rsize,Ssize,selectivity,double=False)
    #db_to_file(R,32,"Run1","R")
    #db_to_file(S,32,"Run1","S")
    #cartesian_product_file("Run1",memory,pageSize)
    sort_file("Run1",memory,pageSize)

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



#algo sort_merge doublon
'''
while iR<n :
        read+=1 # le prochain Y
        if iS==m :
            if R['Y'].get(iR+1)==S['Y'].get(iS-1) :
                iS-=1
            
                while R['Y'].get(iR)==S['Y'].get(iS): #gère le cas des doublons
                    iS-=1
                    read+=1

                iS+=1
                iR+=1
                read+=2 #on incrémente des deux cotes
                
            else :
                
                return pd.DataFrame(T,columns=['X','Y','Z']),read1+read2,written1+written2,read,written
        elif R['Y'].get(iR)==S['Y'].get(iS): 
            T.append((R['X'].get(iR),R['Y'].get(iR),S['Z'].get(iS)))
            written+=1
            iS+=1
            
        elif R['Y'].get(iR)>S['Y'].get(iS):
            iS+=1
            
        else:
            iS-=1
            
            while R['Y'].get(iR)==S['Y'].get(iS): #gère le cas des doublons
                iS-=1
                read+=1

            iS+=1
            iR+=1
            read+=2 #on incrémente des deux cotes

    T=pd.DataFrame(T,columns=['X','Y','Z'])
'''