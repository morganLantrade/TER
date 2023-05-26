import random
import pandas as pd

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
    '''FOnction recursive qui effectue un tri fusion avec mémorisation des lectures et écritures dans la mémoire'''
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

def generate_db(N,selectivity,double=False):
    '''Renvoi deux dataframe correspondant aux tables R(X,Y) et S(Y,Z)
    N : nombre d'élements dans chaque table
    selectivity : compris entre 0 et 1 , permet de déterminer le pourcentage
                  d'Y en communs de la table S et celle de R
    double : doublon possible ou non
    '''
    relation=int(N*selectivity) 
    R=[]
    S=[]
    if double:
        RY=[random.randint(1,9) for _ in range(N)]
    else:
        RY=[i for i in range(1,N+1)]
        random.shuffle(RY)
    #on récupere les Y en communs aux hasard
    SY=random.sample(RY,relation)

    #on récupere des y inexistants dans R de facon aléatoire et uniforme
    L=[]
    L=[i for i in range(-N+1,1)]
    L+=[i for i in range(N+1,2*N+1)]
    SY+=random.sample(L,N-relation) 
    random.shuffle(SY)
   
    #création des tables
    for i in range(N):
        R.append((i+1,RY[i]))
        S.append((SY[i],i+1))
    R=pd.DataFrame(R,columns=['X','Y'])
    S=pd.DataFrame(S,columns=['Y','Z'])
    return R,S






def sort_merge(R,S):
    '''Renvoi un inner join des tables R et S en utilisant un algorithme de tri fusion'''
    n=len(R)
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
    while iS<N and iR<N :
        read+=1 # le prochain Y
        if R['Y'].get(iR)==S['Y'].get(iS): 
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
    
def cartesian_product(R,S):
    '''Renvoi un inner join des tables R et S en utilisant un algorithme de produit cartésien simple'''
    n=len(R)
    m=len(S)
    T=[]
    written=0
    read=0
    for i in range(n):
        read+=1
        for j in range(m):
            read+=1
            if R['Y'].get(i)==S['Y'].get(j):
                T.append((R['X'].get(i),R['Y'].get(i),S['Z'].get(j)))
                written+=1
    return pd.DataFrame(T,columns=['X','Y','Z']),0,0,read,written


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







if __name__ == '__main__':
    test=[4,5,7,2,1,3,5,4,8]
    
    N=1000
    selectivity=0.8
    R,S=generate_db(N,selectivity,double=False)
    
    
    print("-"*10)
    print("Sort-merge")
    print("-"*10)
    
    A,r0,w0,r1,w1=sort_merge(R,S)
    print(A)
    print("----")
    print("Pre-Traitement:\n")
    print('Lecture :',r0,'/ Ecriture :',w0)
    print("----")
    print("Post-Traitement:\n")
    print('Lecture :',r1,'/ Ecriture :',w1)
    print("--")
    print('Total : Lecture : ',r0+r1,' / Ecriture : ',w0+w1)
    
    #cartesian
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
    #hachage
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
    print('Total : Lecture : ',r0+r1,' / Ecriture : ',w0+w1)