from tools import *
from index import *
from sortMerge import *

def cartesian_product_file(folderName,memory,pageSize):
    '''Jointure cartesienne selon R et S contenu dans le folder
    pour une mémoire centrale et une taille de page donnée'''
    assert memory>=3, "Erreur : La memoire doit contenir au moins 3 pages"

    if not os.path.exists('Data/'+folderName+"_cp"):
        os.makedirs('Data/'+folderName+"_cp")
    else:
        #supprime tous les fichiers du repertoire run correspondant 
        delete_file("T",folderName+"_cp") 

    #metadonnées
    nbPageR=len([f for f in os.listdir("Data/"+folderName) if "R_" in f])
    nbPageS=len([f for f in os.listdir("Data/"+folderName) if "S_" in f])


    seconds=time.time()

    #taille bloc
    b=memory-2
    
    T=[]
    
    

    nbPageT=0
    i=0
    while i<nbPageR:

        if i+b< nbPageR:
            #un bloc de taille b
            R=read_X_pages(folderName+"/R",i+1,b)
        elif i==0: 
            #cas ou tout R rentre dans la mémoire
            R=read_X_pages(folderName+"/R",i+1,nbPageR)
        else: 
            #cas ou on charge les dernieres pages
            R=read_X_pages(folderName+"/R",i+1,nbPageR%i)
            i=nbPageR
        i+=b
                
        for j in range (nbPageS):
            #charge une page de S
            S=read_X_pages(folderName+"/S",j+1,1)
            for k in range(len(R.index)):
                for l in range(len(S.index)):
                    if (R["Y"].get(k)==S["Y"].get(l)):
                        T.append((R['X'].get(k),(R['Y'].get(k),S['Y'].get(l)),S['Z'].get(l)))
                        #buffer plein
                        if len(T)==pageSize:
                            pd.DataFrame(T,columns=['X','Y','Z']).to_csv('Data/'+folderName+"_cp/T_"+str(nbPageT+1)+".csv",sep=',',index=False)
                            nbPageT+=1
                            T=[]
    if T:
        #vide le buffer si il est non vide
        pd.DataFrame(T,columns=['X','Y','Z']).to_csv('Data/'+folderName+"_cp/T_"+str(nbPageT+1)+".csv",sep=',',index=False)
    return time.time()-seconds






def cartesian_product(folderName,selectivity,memory,pageSize):
    '''Renvoi les lectures et ecritures theoriques d'un algorithme de produit cartésien par block'''
    assert memory>=3, "Erreur : La memoire doit contenir au moins 3 pages"
    
    
    #metadonnées
    R_pages=len([f for f in os.listdir("Data/"+folderName) if "R_" in f])
    S_pages=len([f for f in os.listdir("Data/"+folderName) if "S_" in f])
    nbTuplesR=((R_pages-1)*pageSize)+len(read_X_pages(folderName+"/R",R_pages,1).index)
    
     #taille bloc
    b=memory-2
    
    th_read= R_pages  + S_pages*math.ceil(R_pages/b)
    th_written= math.ceil(math.ceil(nbTuplesR*selectivity)/pageSize)
  
    return th_read,th_written




def cartesian_product_index_file(folderName,memory,pageSize):
    '''Jointure cartesienne selon R et S contenu dans le folder
    pour une mémoire centrale et une taille de page donnée'''
    assert memory>=4, "Erreur : La memoire doit contenir au moins 4 pages"

    if not os.path.exists('Data/'+folderName+"_cpi"):
        os.makedirs('Data/'+folderName+"_cpi")
    else:
        #supprime tous les fichiers du repertoire run correspondant 
        delete_file("T",folderName+"_cpi") 
    
    #metadonnées
    nbPageS=len([f for f in os.listdir("Data/"+folderName) if "S_" in f])
    nbPageI=len([f for f in os.listdir("Data/"+folderName+"_idx2") if "I" in f])


    #Build
    level,passe,timer=index_to_file2(folderName,"R",memory,pageSize)

    seconds=time.time()

    #initialisation
    T=[]
    iT=1
    ram=dict()
    stat=dict()
    free_space=memory-3
    
    for PageS in range(1,nbPageS+1):
        S=read_X_pages(folderName+"/S",PageS,1)
        for i in range(len(S.index)):
            y,z = int(S["Y"].get(i)),int(S["Z"].get(i))
            search,ram,stat,free_space =search_index(folderName,ram,free_space,level,y,stat,nbPageI)
            
            if search is not None:

                page=(search+1)//pageSize +int((search+1)%pageSize!=0)
                line= search%pageSize +2
                
                
                x,y1=read_line(folderName+"_sorted/R"+str(passe)+"_0",page,line)
                
                T.append((x,(y1,y),z))
                #buffer plein
                if len(T)==pageSize:
                    pd.DataFrame(T,columns=['X','Y','Z']).to_csv('Data/'+folderName+"_cpi/T_"+str(iT)+".csv",sep=',',index=False)
                    iT+=1
                    T=[]



    if T:
        #vide le buffer si il est non vide
        pd.DataFrame(T,columns=['X','Y','Z']).to_csv('Data/'+folderName+"_cpi/T_"+str(iT)+".csv",sep=',',index=False)
    return timer+(time.time()-seconds)
    
    

def cartesian_product_index_cost(nbTuplesR,nbTuplesS,selectivity,memory,pageSize):
    '''Retourne le nombre de lectures,ecritures et le cout en temps(ms) du cartesian_product_index_cost'''
    
    #metadonnées
    R_pages=math.ceil(nbTuplesR/pageSize)
    S_pages=math.ceil(nbTuplesS/pageSize)
    
    #Nombre de niveaux et pages par niveaux
    idx=dict()
    #Dernier niveau
    idx[-1]=R_pages
    #Les autres niveaux
    dic=nbLevel(R_pages,pageSize)
    for k,v in dic.items():
        idx[k]=v
    
            
    #Build
    rR,wR,cR=sort_cost(nbTuplesR,memory,pageSize)
    
    read_build = R_pages+rR
    written_build = sum(dic.values()) +wR
    cost_build = cR + (nbTuplesR * MOVE)/1000
    

    
    # nombre de niveau 
    n=len(idx)-1
    free_space=memory-3
    read=0
    #tant qu'on peut stocker les niveaux de l'index dans la mémoire on réduit le nombre de niveau a charger
    while n>=0 and free_space-idx[(n-1)]>=0 :  
        free_space-= idx[(n-1)]
        n-=1
    
        
    #probe
    read_probe=  S_pages + nbTuplesS* (n+1)
    write_probe = math.ceil(R_pages*selectivity)
    cost_probe= nbTuplesS*COMP*(len(idx))  + (read_probe*IO_READ+write_probe*IO_WRITE) + math.ceil(nbTuplesR*selectivity)*MOVE
    read=read_probe+read_build
    write=written_build+write_probe
    
            
    return read,write,cost_build,cost_probe/1000


def cartesian_product_index_cost2(nbTuplesR,nbTuplesS,selectivity,memory,pageSize):
    '''Retourne le nombre de lectures,ecritures et le cout en temps(ms) du cartesian_product_index_cost'''
    
    #metadonnées
    R_pages=math.ceil(nbTuplesR/pageSize)
    S_pages=math.ceil(nbTuplesS/pageSize)
    
    
    #Nombre de niveaux et pages par niveaux
    idx=dict()
    #Dernier niveau
    idx[-1]=S_pages
    #Les autres niveaux
    dic=nbLevel(S_pages,pageSize)
    for k,v in dic.items():
        idx[k]=v
    
            
    #Build
    rR,wR,cR=sort_cost(nbTuplesS,memory,pageSize)
    
    read_build = S_pages+rR
    written_build = sum(dic.values()) +wR
    cost_build = cR + (nbTuplesS * MOVE)/1000

    
    
    # nombre de niveau 
    n=len(idx)-1
    free_space=memory-3
    read=0
    #tant qu'on peut stocker les niveaux de l'index dans la mémoire on réduit le nombre de niveau a charger
    while n>=0 and free_space-idx[(n-1)]>=0 :  
        free_space-= idx[(n-1)]
        n-=1
    
        
    #probe
    read_probe=  R_pages + nbTuplesR* (n+1)
    write_probe = math.ceil(R_pages*selectivity)
    cost_probe= nbTuplesR*COMP*(len(idx))  + (read_probe*IO_READ+write_probe*IO_WRITE) + math.ceil(nbTuplesR*selectivity)*MOVE
    read=read_probe+read_build
    write=written_build+write_probe
    
            
    return read,write,cost_build,cost_probe/1000