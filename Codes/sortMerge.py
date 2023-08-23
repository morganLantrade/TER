from tools import *



def sort_file(folderName,memory,pageSize,dbName):
    '''Effectue un tri externe du fichier dbName de la run folderName selon la memoire et la taille des pages'''
    assert memory>=3, "Erreur : La memoire doit contenir au moins 3 pages"

    if not os.path.exists('Data/'+folderName+"_sorted"):
        os.makedirs('Data/'+folderName+"_sorted")
    else:
        #vide le contenu
        delete_file(dbName,folderName+"_sorted",)

    #metadonnee
    nbPage=len([f for f in os.listdir("Data/"+folderName) if dbName in f])

    seconds=time.time()

    nbMonotonie=0

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
    passe=1
    a,b = ("X","Y") if dbName =="R"  else ("Y","Z")

    while nbMonotonie!=1: #derniere passe
        
        nbPageMonotonie=(memory*((memory-1)**(passe-1)))
        #parcours des monotonies
        
        for i in range(0,nbMonotonie,memory-1):
            buffer=[]
            iB=1

            if (i==nbMonotonie-(nbMonotonie%(memory-1))) :
                nbPageLastMonotonie=(nbPageMonotonie if nbPage%nbPageMonotonie==0 else nbPage%nbPageMonotonie)
                if (nbMonotonie%(memory-1))==1:
                    for j in range(nbPageLastMonotonie):
                        db=read_X_pages(folderName+"_sorted/"+dbName+str(passe-1)+"_"+str(i),1,1)
                        db.to_csv('Data/'+folderName+"_sorted/"+dbName+str(passe)+"_"+str(i//(memory-1))+"_"+str(iB)+".csv",sep=',',index=False)
                        iB+=1
                else:
                    LDB=[read_X_pages(folderName+"_sorted/"+dbName+str(passe-1)+"_"+str(i+j),1,1) for j in range(nbMonotonie%(memory-1))]
                    LiPage=[1]*((nbMonotonie%(memory-1)))
                    LiTuple=[0]*((nbMonotonie%(memory-1)))
                    iLastMonotonie=(nbMonotonie%(memory-1))-1
                    while LDB:
                        L=[LDB[j]["Y"].get(LiTuple[j]) for j in range(len(LDB))]
                        indexMin=indexOfMin(L)
                        buffer.append((LDB[indexMin][a].get(LiTuple[indexMin]),LDB[indexMin][b].get(LiTuple[indexMin])))
                        if len(buffer)==pageSize:
                            buffer=pd.DataFrame(buffer,columns=[a,b])
                            buffer.to_csv('Data/'+folderName+"_sorted/"+dbName+str(passe)+"_"+str(i//(memory-1))+"_"+str(iB)+".csv",sep=',',index=False)
                            iB+=1
                            buffer=[]
                        LiTuple[indexMin]+=1
                        if indexMin==iLastMonotonie:
                            #si la page est lue
                            if LiTuple[indexMin]==len(LDB[indexMin].index): 
                                LiTuple[indexMin]=0
                                LiPage[indexMin]+=1
                                #si c'était la dernière page de cette monotonie
                                if LiPage[indexMin]==nbPageLastMonotonie+1:
                                    del LDB[indexMin]
                                    del LiPage[indexMin]
                                    del LiTuple[indexMin]
                                    iLastMonotonie=-1
                                #sinon on charge la prochaine page de cette monotonie
                                else:
                                    LDB[indexMin]=read_X_pages(folderName+"_sorted/"+dbName+str(passe-1)+"_"+str(i+indexMin),LiPage[indexMin],1)
                        else:
                            #si la page est lue
                            if LiTuple[indexMin]==pageSize:
                                LiTuple[indexMin]=0 
                                LiPage[indexMin]+=1
                                #si c'était la dernière page de cette monotonie
                                if LiPage[indexMin]==nbPageMonotonie+1:
                                    del LDB[indexMin]
                                    del LiPage[indexMin]
                                    del LiTuple[indexMin]
                                    iLastMonotonie-=1
                                #sinon on charge la prochaine page de cette monotonie
                                else:
                                    LDB[indexMin]=read_X_pages(folderName+"_sorted/"+dbName+str(passe-1)+"_"+str(i+indexMin),LiPage[indexMin],1)
                    if buffer :
                        buffer=pd.DataFrame(buffer,columns=[a,b])
                        buffer.to_csv('Data/'+folderName+"_sorted/"+dbName+str(passe)+"_"+str(i//(memory-1))+"_"+str(iB)+".csv",sep=',',index=False)
            elif i==nbMonotonie-(memory-1):
                nbPageLastMonotonie=(nbPageMonotonie if nbPage%nbPageMonotonie==0 else nbPage%nbPageMonotonie)
                LDB=[read_X_pages(folderName+"_sorted/"+dbName+str(passe-1)+"_"+str(i+j),1,1) for j in range(memory-1)]
                LiPage=[1]*(memory-1)
                LiTuple=[0]*(memory-1)
                iLastMonotonie=memory-2
                while LDB:
                    L=[LDB[j]["Y"].get(LiTuple[j]) for j in range(len(LDB))]
                    indexMin=indexOfMin(L)
                    buffer.append((LDB[indexMin][a].get(LiTuple[indexMin]),LDB[indexMin][b].get(LiTuple[indexMin])))
                    if len(buffer)==pageSize:
                        buffer=pd.DataFrame(buffer,columns=[a,b])
                        buffer.to_csv('Data/'+folderName+"_sorted/"+dbName+str(passe)+"_"+str(i//(memory-1))+"_"+str(iB)+".csv",sep=',',index=False)
                        iB+=1
                        buffer=[]
                    LiTuple[indexMin]+=1
                    if indexMin==iLastMonotonie:
                        #si la page est lue
                        if LiTuple[indexMin]==len(LDB[indexMin].index): 
                            LiTuple[indexMin]=0
                            LiPage[indexMin]+=1
                            #si c'était la dernière page de cette monotonie
                            if LiPage[indexMin]==nbPageLastMonotonie+1:
                                del LDB[indexMin]
                                del LiPage[indexMin]
                                del LiTuple[indexMin]
                                iLastMonotonie=-1
                            #sinon on charge la prochaine page de cette monotonie
                            else:
                                LDB[indexMin]=read_X_pages(folderName+"_sorted/"+dbName+str(passe-1)+"_"+str(i+indexMin),LiPage[indexMin],1)
                    else:
                        #si la page est lue
                        if LiTuple[indexMin]==pageSize: 
                            LiTuple[indexMin]=0
                            LiPage[indexMin]+=1
                            #si c'était la dernière page de cette monotonie
                            if LiPage[indexMin]==nbPageMonotonie+1:
                                del LDB[indexMin]
                                del LiPage[indexMin]
                                del LiTuple[indexMin]
                                iLastMonotonie-=1
                            #sinon on charge la prochaine page de cette monotonie
                            else:
                                LDB[indexMin]=read_X_pages(folderName+"_sorted/"+dbName+str(passe-1)+"_"+str(i+indexMin),LiPage[indexMin],1)
                if buffer :
                    buffer=pd.DataFrame(buffer,columns=[a,b])
                    buffer.to_csv('Data/'+folderName+"_sorted/"+dbName+str(passe)+"_"+str(i//(memory-1))+"_"+str(iB)+".csv",sep=',',index=False)
                
            else:
                LDB=[read_X_pages(folderName+"_sorted/"+dbName+str(passe-1)+"_"+str(i+j),1,1) for j in range(memory-1)]
                LiPage=[1]*(memory-1)
                LiTuple=[0]*(memory-1)
                while LDB:
                    L=[LDB[j]["Y"].get(LiTuple[j]) for j in range(len(LDB))]
                    indexMin=indexOfMin(L)
                    buffer.append((LDB[indexMin][a].get(LiTuple[indexMin]),LDB[indexMin][b].get(LiTuple[indexMin])))
                    if len(buffer)==pageSize:
                        buffer=pd.DataFrame(buffer,columns=[a,b])
                        buffer.to_csv('Data/'+folderName+"_sorted/"+dbName+str(passe)+"_"+str(i//(memory-1))+"_"+str(iB)+".csv",sep=',',index=False)
                        iB+=1
                        buffer=[]
                    LiTuple[indexMin]+=1
                    #si la page est lue
                    if LiTuple[indexMin]==pageSize:
                        LiTuple[indexMin]=0 
                        LiPage[indexMin]+=1
                        #si c'était la dernière page de cette monotonie
                        if LiPage[indexMin]==nbPageMonotonie+1:
                            del LDB[indexMin]
                            del LiPage[indexMin]
                            del LiTuple[indexMin]
                        #sinon on charge la prochaine page de cette monotonie
                        else:
                            LDB[indexMin]=read_X_pages(folderName+"_sorted/"+dbName+str(passe-1)+"_"+str(i+indexMin),LiPage[indexMin],1)
                if buffer :
                    buffer=pd.DataFrame(buffer,columns=[a,b])
                    buffer.to_csv('Data/'+folderName+"_sorted/"+dbName+str(passe)+"_"+str(i//(memory-1))+"_"+str(iB)+".csv",sep=',',index=False)
    
        #passe suivante
        nbMonotonie=math.ceil(nbMonotonie/(memory-1))
        passe+=1

    
    return passe-1,time.time()-seconds

def sort_merge_file(folderName,memory,pageSize):
    '''Effectue un join de S et R contenus dans la run foldername selon la memoire et la taille de page'''
    
    if not os.path.exists('Data/'+folderName+"_sm"):
        os.makedirs('Data/'+folderName+"_sm")
    else:
        #vide le contenu
        delete_file("T",folderName+"_sm",)

    
    #Tri les fichiers sur disque et revoie le nombre de passes pour chaque relation
    passeR,timer=sort_file(folderName,memory,pageSize,"R")
    passeS,timer2=sort_file(folderName,memory,pageSize,"S")
    timer+=timer2

    #metadonnees
    nbPageR=len([f for f in os.listdir("Data/"+folderName+"_sorted") if ("R"+str(passeR)) in f])
    nbPageS=len([f for f in os.listdir("Data/"+folderName+"_sorted") if ("S"+str(passeS)) in f])
    
    seconds=time.time()

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
    return timer+(time.time()-seconds)

    
def sort_cost(nbTuples,memory,pageSize):
    '''Retourne le nombre de lectures,ecritures et le cout en temps(ms) de tri externe de dbName'''
    #metadonnées
    pages=math.ceil(nbTuples/pageSize)
    
    nb_pass = 1+math.ceil(math.log(math.ceil(pages/memory),memory-1))
    r=w=pages*nb_pass 
    cost= nbTuples * math.log(nbTuples,2) * (MOVE+SWAP) + (r*IO_READ+w*IO_WRITE)

    return r,w,cost/1000
    

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
    read_probe=R_pages+S_pages
    write_probe=math.ceil(R_pages*selectivity)
    cost_probe=(nbTuplesR+nbTuplesS)*COMP+(read_probe*IO_READ+write_probe*IO_WRITE)+ math.ceil(nbTuplesR)*selectivity 

    
    return read_build+read_probe,written_build+write_probe,cost_build,cost_probe/1000