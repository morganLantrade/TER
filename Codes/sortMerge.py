from tools import *


@profile
def sort_file(folderName,memory,pageSize,dbName):
    '''Effectue un tri externe du fichier dbName de la run folderName selon la memoire et la taille des pages'''
    assert memory>=3, "Erreur : La memoire doit contenir au moins 3 pages"

    #metadonnee
    nbPage=len([f for f in os.listdir("Data/"+folderName) if dbName in f])

    if not os.path.exists('Data/'+folderName+"_sorted"):
        os.makedirs('Data/'+folderName+"_sorted")
    else:
        #vide le contenu
        delete_file(dbName,folderName+"_sorted",)

    nbMonotonie=0
    passe=0

    ###########
    # PASSE 0 #
    ###########
    #parcours par bloc de taille memoire
    for i in range(0,nbPage,memory):
        #dernier bloc
        if (i==nbPage-(nbPage%memory)):
            db=read_X_pages(folderName+"/"+dbName,(nbPage-(nbPage%memory))+1,nbPage%memory)
            #tri quicksort de tout le bloc
            db=db.sort_values(by=['Y'],kind='quicksort')
            #ecriture du bloc en page
            db_to_file(db,pageSize,folderName+"_sorted",dbName+"0_"+str(nbPage//memory))
            nbMonotonie+=1
        #tous les blocs de taille memoire
        else:
            db=read_X_pages(folderName+"/"+dbName,i+1,memory)
            #tri quicksort de tout le bloc
            db=db.sort_values(by=['Y'],kind='quicksort')
            #ecriture du bloc en page
            db_to_file(db,pageSize,folderName+"_sorted",dbName+"0_"+str(i//memory))
            nbMonotonie+=1

    ####################
    # PASSES SUIVANTES #
    ####################
    while nbMonotonie!=1: #derniere passe
        nbPageMonotonie=(memory*((memory-1)**passe))
        #parcours des monotonies
        for i in range(math.ceil(nbMonotonie/(memory-1))):
            L=[]
            #regroupement de memoire-1 monotonies
            for j in range(memory-1):
                #si toutes les pages de R ont étées lues
                if nbPage>(j+i*(memory-1))*nbPageMonotonie:
                    #dernier bloc de monotonies
                    if (j+i*(memory-1)==nbMonotonie-1) and ((nbPage%(memory*((memory-1)**(passe))))!=0):
                        db=read_X_pages(folderName+"_sorted/"+dbName+str(passe)+"_"+str(j+i*(memory-1)),1,nbPage%nbPageMonotonie)
                    #tous les blocs de monotonies de taille normale
                    else:
                        db=read_X_pages(folderName+"_sorted/"+dbName+str(passe)+"_"+str(j+i*(memory-1)),1,nbPageMonotonie)
                    L.append(db)
            db=pd.concat(L, axis=0,ignore_index=True)
            #mergesort du bloc
            db=db.sort_values(by=['Y'],kind='mergesort',ignore_index=True)
            #ecriture du bloc trie en page
            db_to_file(db,pageSize,folderName+"_sorted",dbName+str(passe+1)+"_"+str(i))
        #passe suivante
        nbMonotonie=math.ceil(nbMonotonie/(memory-1))
        passe+=1
    return passe

@profile
def sort_merge_file(folderName,memory,pageSize):
    '''Effectue un join de S et R contenus dans la run foldername selon la memoire et la taille de page'''
    
    if not os.path.exists('Data/'+folderName+"_sm"):
        os.makedirs('Data/'+folderName+"_sm")
    else:
        #vide le contenu
        delete_file("T",folderName+"_sm",)

    #Tri les fichiers sur disque et revoie le nombre de passes pour chaque relation
    passeR=sort_file(folderName,memory,pageSize,"R")
    passeS=sort_file(folderName,memory,pageSize,"S")
    
    #metadonnees
    nbPageR=len([f for f in os.listdir("Data/"+folderName+"_sorted") if ("R"+str(passeR)) in f])
    nbPageS=len([f for f in os.listdir("Data/"+folderName+"_sorted") if ("S"+str(passeS)) in f])

    #Initialisation
    iPageR=1
    iPageS=1
    iT=1
    R=read_X_pages(folderName+"_sorted/R"+str(passeR)+"_0",iPageR,1)
    S=read_X_pages(folderName+"_sorted/S"+str(passeS)+"_0",iPageS,1)
    
    iS=0
    iR=0
    T=[]
    #Tant qu'on a pas fini de parcourir une relation
    while iPageR<nbPageR+1 and iPageS<nbPageS+1:
        #Tant qu'on a pas fini un page
        while (iS<len(S.index) and iR<len(R.index)):
            if R['Y'].get(iR)==S['Y'].get(iS): 
                T.append((R['X'].get(iR),R['Y'].get(iR),S['Z'].get(iS)))
                #vide le buffer si plein
                if (len(T)==pageSize):
                    T=pd.DataFrame(T,columns=['X','Y','Z'])
                    T.to_csv('Data/'+folderName+"_sm/T_"+str(iT)+".csv",sep=',',index=False)
                    iT+=1
                    T=[]
                iS+=1
                iR+=1
                
            elif R['Y'].get(iR)>S['Y'].get(iS):
                iS+=1
                
            else:
                iR+=1
        #page S finie
        if iS==len(S.index):
            iPageS+=1
            #si la page existe on charge la suivante
            if iPageS<=nbPageS:
                S=read_X_pages(folderName+"_sorted/S"+str(passeS)+"_0",iPageS,1)
                iS=0
        #page R finie
        else:
            iPageR+=1
            #si la page existe on charge la suivante
            if iPageR<=nbPageR:
                R=read_X_pages(folderName+"_sorted/R"+str(passeR)+"_0",iPageR,1)
                iR=0
    #vide le buffer si non vide
    if T:
        T=pd.DataFrame(T,columns=['X','Y','Z'])
        T.to_csv('Data/'+folderName+"_sm/T_"+str(iT)+".csv",sep=',',index=False)

    
def sort_cost(nbTuples,memory,pageSize):
    '''Retourne le nombre de lectures,ecritures et le cout en temps(ms) de tri externe de dbName'''
    #metadonnées
    pages=math.ceil(nbTuples/pageSize)
    
    nb_pass = 1+math.ceil(math.log(math.ceil(pages/memory),memory-1))
    r=w=pages*nb_pass 
    cost= nbTuples * math.log(nbTuples,2) * (MOVE+SWAP) + (r+w)*IO

    return r,w,cost
    

def sort_merge_join_cost(nbTuplesR,nbTuplesS,selectivity,memory,pageSize):
    '''Retourne le nombre de lectures,ecritures et le cout en temps(ms) de sort_merge_join'''
    
    #metadonnées
    R_pages=math.ceil(nbTuplesR/pageSize)
    S_pages=math.ceil(nbTuplesS/pageSize)
    
    
    #Build
    rR,wR,cR=sort_cost(nbTuplesR,memory,pageSize)
    rS,wS,cS=sort_cost(nbTuplesS,memory,pageSize)
    read_build,written_build,cost_build= rR+rS , wR+wS, cR+cS
    
   
    #Probe
    read_probe=write_probe=R_pages+S_pages
    cost= cR+cS + (nbTuplesR+nbTuplesS)*COMP + (read_build+written_build)*IO + math.ceil(nbTuplesR)*selectivity

    
    return read_build+read_probe,written_build+write_probe,cost