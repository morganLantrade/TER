from tools import *
from index import *

def cartesian_product_file(folderName,memory,pageSize):
    '''Jointure cartesienne selon R et S contenu dans le folder
    pour une mémoire centrale et une taille de page donnée'''
    assert memory>=3, "Erreur : La memoire doit contenir au moins 3 pages"

    #metadonnées
    nbPageR=len([f for f in os.listdir("Data/"+folderName) if "R_" in f])
    nbPageS=len([f for f in os.listdir("Data/"+folderName) if "S_" in f])

    #taille bloc
    b=memory-2
    
    T=[]
    path='Data/'+folderName+"_cp"
    
    if not os.path.exists(path):
        os.makedirs(path)
    else:
        #supprime tous les fichiers du repertoire run correspondant 
        delete_file("T",folderName+"_cp") 
    

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
                        T.append((R['X'].get(k),R['Y'].get(k),S['Z'].get(l)))
                        #buffer plein
                        if len(T)==pageSize:
                            pd.DataFrame(T,columns=['X','Y','Z']).to_csv('Data/'+folderName+"_cp/T_"+str(nbPageT+1)+".csv",sep=',',index=False)
                            nbPageT+=1
                            T=[]
    if T:
        #vide le buffer si il est non vide
        pd.DataFrame(T,columns=['X','Y','Z']).to_csv('Data/'+folderName+"_cp/T_"+str(nbPageT+1)+".csv",sep=',',index=False)






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

def cartesian_product_index(folderName,selectivity,memory,pageSize):
    '''Renvoi une simulation memoire d'un join des tables R et S en utilisant un algorithme de produit cartésien indexe sur S'''
    assert memory>=4, "Erreur : La memoire doit contenir au moins 4 pages"
    
    #metadonnées
    R_pages=len([f for f in os.listdir("Data/"+folderName) if "R_" in f])
    S_pages=len([f for f in os.listdir("Data/"+folderName) if "S_" in f])
    nbTuplesS=((S_pages-1)*pageSize)+len(read_X_pages(folderName+"/S",S_pages,1).index)
    nbTuplesR=((R_pages-1)*pageSize)+len(read_X_pages(folderName+"/R",R_pages,1).index)
    idx=nbLevel(R_pages,pageSize)

    print("Pages par niveau de l'index :",idx)

        
    #Build
    read_build_th = R_pages 
    #nombre pages index
    written_build_th = sum(idx.values()) 
    
    #Probe
    n=len(idx)# nombre de niveau a charger regulierement
    free_space=memory-3
    read=0
    #tant qu'on peut stocker les niveaux de l'index dans la mémoire on réduit le nombre de niveau a charger
    while free_space-idx[(n-1)]>=0 and n>0:  
        free_space-= idx[(n-1)]
        read+=idx[(n-1)] #charge au moins une fois
        n-=1
    

    #probe de s  + #les les tuples correspondant a E
    read_probe_th = S_pages + nbTuplesS*n + selectivity*nbTuplesR  + read
    write_probe_th =  math.ceil(math.ceil(nbTuplesR*selectivity)/pageSize)
    
    
    
    return read_build_th+read_probe_th,written_build_th+write_probe_th
