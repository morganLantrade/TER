import random
import pandas as pd
import math

def merge_sort(arr,i):
    '''Fonction qui appelle la fonction recursive merge qui renvoie la liste arr triée 
    et le nombre de lecture et ecriture dans la memoire'''
    read_count = 0
    write_count = 0

    if len(arr) <= 1:
        return arr, read_count, write_count

    mid = len(arr) // 2
    left_half = arr[:mid]
    right_half = arr[mid:]

    left_half, left_reads, left_writes = merge_sort(left_half,i)
    right_half, right_reads, right_writes = merge_sort(right_half,i)

    read_count += left_reads + right_reads
    write_count += left_writes + right_writes

    sorted_arr, merge_reads, merge_writes = merge(left_half, right_half,i)
    read_count += merge_reads
    write_count += merge_writes

    return sorted_arr, read_count, write_count


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

        relation=int(Rsize*selectivity) 

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


def sort_merge_join(R,S):
    '''Renvoi un inner join des tables R et S en utilisant un algorithme de tri fusion'''
    n=len(R)
    m=len(S)
    # Tri des deux tables au préalable
    
    R,read1,written1=merge_sort(R.values.tolist(),1)
    S,read2,written2=merge_sort(S.values.tolist(),0)
    R=pd.DataFrame(R,columns=['X','Y'])
    S=pd.DataFrame(S,columns=['Y','Z'])
    T=[]
    iR=0
    iS=0
    written=0
    read=2 #les deux premier Y
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
    return pd.DataFrame(T,columns=['X','Y','Z']),read1+read2,written1+written2,read,written
    
def cartesian_product(R,S,selectivity,memory,size_of_tuple,size_of_page):
    '''Renvoi un  join des tables R et S en utilisant un algorithme de produit cartésien par block'''
    assert memory>=3, "Erreur : La memoire doit contenir au moins 3 pages"
    
    
    ##Theoric##
    R_pages,_= number_of_pages(R,size_of_tuple,size_of_page)
    S_pages,_=number_of_pages(S,size_of_tuple,size_of_page)
    tuples_per_page= size_of_page//size_of_tuple
    b=memory-2 #taille bloc
    th_read= R_pages  + S_pages*math.ceil(R_pages/b)
    th_written= math.ceil(int(len(R)*selectivity)/tuples_per_page)
    
        
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
    write_probe_th = written= math.ceil(int(len(R)*selectivity)/tuples_per_page)
    


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



def hash1(x):
    return x%3

def fnv_1a_hash_numeric(data):
    '''Fonction de hachage fnv_1a (source : wikipedia/chat GPT)
    Se renseigner dessus sur les perfomances collisions/mémoires'''
    FNV_OFFSET_BASIS = 0x811C9DC5
    FNV_PRIME = 0x01000193
    hash_value = FNV_OFFSET_BASIS

    # Convertir le nombre en octets
    byte_data = bytes(str(data), 'utf-8')

    for byte in byte_data:
        hash_value ^= byte
        hash_value *= FNV_PRIME

    return hash_value & 0xFFFFFFFF



def hash_join(R,S,hash_function=fnv_1a_hash_numeric):
    '''Renvoi un inner join des tables R et S en utilisant un algorithme de hachage simple
    collisions gérées par chainage'''
    H=dict()
    n=len(R)
    m=len(S)
    T=[]
    read0=0
    written0=0
    #Remplissage de la table de hachage de R
    for i in range(n):
        x,y=R['X'].get(i),R['Y'].get(i)
        read0+=2
        key=hash_function(y)
        if key in H:
            H[key].append((x,y))
        else:
            H[key]=[(x,y)]
        written0+=1
    
    written=0
    read=0
    # Parcours de S
    for j in range(m):
        y,z=S['Y'].get(j),S['Z'].get(j)
        read+=2 # on lit y de S + la clé
        key=hash_function(y)
        if key in H: #la clé existe on parcour toute la chaine
            for (Rx,Ry) in H[key]:
                read+=1 # un autre tuple
                if y==Ry:
                    written+=1
                    T.append((Rx,Ry,z))
    return pd.DataFrame(T,columns=['X','Y','Z']),read0,written0,read,written



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

def test_cartesian_product_index(R,S,selectivity,memory,size_of_tuple,size_of_page,size_key_index):
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
    

if __name__ == '__main__':
    
    Rsize=1000
    Ssize=2000
    selectivity=0.8
    #test_cartesian_product(Rsize=1000,Ssize=2000,selectivity=0.25,memory=3,size_of_tuple=32,size_of_page=1024)
    test_cartesian_product_index(Rsize,Ssize,selectivity=0.25,memory=7,size_of_tuple=32,size_of_page=1024,size_key_index=8)
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
