import random
import pandas as pd



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
    R=R.sort_values(by=['Y'])
    S=S.sort_values(by=['Y'])
    R=R.reset_index(drop=True)
    S=S.reset_index(drop=True)
    T=[]
    iR=0
    iS=0
    while iS<N and iR<N :
        if R['Y'].get(iR)==S['Y'].get(iS): 
            T.append((R['X'].get(iR),R['Y'].get(iR),S['Z'].get(iS)))
            iS+=1
        elif R['Y'].get(iR)>S['Y'].get(iS):
            iS+=1
        else:
            iS-=1
            while R['Y'].get(iR)==S['Y'].get(iS): #gère le cas des doublons
                iS-=1
            iS+=1
            iR+=1
    return pd.DataFrame(T,columns=['X','Y','Z'])
    
def cartesian_product(R,S):
    '''Renvoi un inner join des tables R et S en utilisant un algorithme de produit cartésien simple'''
    n=len(R)
    m=len(S)
    T=[]
    for i in range(n):
        for j in range(m):
            if R['Y'].get(i)==S['Y'].get(j):
                T.append((R['X'].get(i),R['Y'].get(i),S['Z'].get(j)))
    return pd.DataFrame(T,columns=['X','Y','Z'])


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



def hash_join(R,S,hash_function=hash1):
    '''Renvoi un inner join des tables R et S en utilisant un algorithme de hachage simple
    collisions gérées par chainage'''
    H=dict()
    n=len(R)
    m=len(S)
    T=[]
    #Remplissage de la table de hachage de R
    for i in range(n):
        x,y=R['X'].get(i),R['Y'].get(i)
        key=hash_function(y)
        if key in H:
            H[key].append((x,y))
        else:
            H[key]=[(x,y)]
    print(H)
    # Parcours de S
    for j in range(m):
        y,z=S['Y'].get(j),S['Z'].get(j)
        key=hash_function(y)
        if key in H: #la clé existe on parcour toute la chaine
            for (Rx,Ry) in H[key]:
                if y==Ry:
                    T.append((Rx,Ry,z))
    return pd.DataFrame(T,columns=['X','Y','Z'])


if __name__ == '__main__':
    N=10
    selectivity=0.8
    R,S=generate_db(N,selectivity,double=False)

    print(R.sort_values(by=['Y']))
    print()
    print(S.sort_values(by=['Y']))
    print()
    print("-"*10)
    print("Sort-merge")
    print("-"*10)
    print(sort_merge(R,S))
    print("-"*10)
    print("Cartesian")
    print("-"*10)
    print(cartesian_product(R,S))
    print("-"*10)
    print("Simple Hash")
    print("-"*10)
    print(hash_join(R,S))
    '''print(R)
    print()
    print(S)
    print()
    print(cartesian_product(R,S))'''

    #print(R.join(S.set_index('Y'), on='Y',how='inner'))