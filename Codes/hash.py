from tools import *
import time

def hybrid_hash_join_file(folderName,memory,pageSize):

    assert memory>=3, "Erreur : La memoire doit contenir au moins 3 pages"

    #métadonnées
    nbPageR=len([f for f in os.listdir("Data/"+folderName) if ("R") in f])
    nbPageS=len([f for f in os.listdir("Data/"+folderName) if ("S") in f])

    seconds=time.time()

    B=1+math.ceil((nbPageR-memory+2)/(memory-3))


    if nbPageR<=memory-2:
        return (time.time()-seconds)+simple_hash_join_file(folderName,memory,pageSize)
    elif B<=memory-2:

        
        timer=time.time()-seconds
        
        if not os.path.exists('Data/'+folderName+"_hybrid_hash"):
            os.makedirs('Data/'+folderName+"_hybrid_hash")
        else:
            #vide le contenu
            delete_file("T",folderName+"_hybrid_hash")


        if not os.path.exists('Data/'+folderName+"_hybrid_hash_partition"):
            os.makedirs('Data/'+folderName+"_hybrid_hash_partition")
        else:
            #vide le contenu
            for f in os.listdir("Data/"+folderName+"_hybrid_hash_partition"):
                delete_folder(folderName+"_hybrid_hash_partition/"+f)
        
        
        for i in range(B-1):
            os.makedirs('Data/'+folderName+"_hybrid_hash_partition/"+str(i+1))
        
        seconds=time.time()

        PSize1=memory-2-(B-1)
        ratioP1=PSize1
        ratioP=math.ceil((nbPageR-ratioP1)/(B-1))
        buffers=[[] for _ in range(B-1)]
        ibuffers=[0 for _ in range(B-1)]
        H=dict()
        T=[]
        iT=1
        for pageR in range(nbPageR):
            db=read_X_pages(folderName+"/R",pageR+1,1) 
            for i in range(len(db.index)):
                key=int(db["Y"].get(i))
                if key%nbPageR<ratioP1:
                    H[key]=db["X"].get(i)
                else:
                    iP=((key%nbPageR)-ratioP1)//ratioP
                    buffers[iP].append((db["X"].get(i),db["Y"].get(i)))
                    if len(buffers[iP])==pageSize:
                        Temp=pd.DataFrame(buffers[iP],columns=['X','Y'])
                        Temp.to_csv('Data/'+folderName+"_hybrid_hash_partition/"+str(iP+1)+"/R_"+str(ibuffers[iP])+".csv",sep=',',index=False)
                        ibuffers[iP]+=1
                        buffers[iP]=[]
        for iP,P in enumerate(buffers):

            if P:
                Temp=pd.DataFrame(P,columns=['X','Y'])
                Temp.to_csv('Data/'+folderName+"_hybrid_hash_partition/"+str(iP+1)+"/R_"+str(ibuffers[iP])+".csv",sep=',',index=False)
            ibuffers[iP]=0
            buffers[iP]=[]

        for pageS in range(nbPageS):
            db=read_X_pages(folderName+"/S",pageS+1,1) 
            for i in range(len(db.index)):
                key=int(db["Y"].get(i))
                if key%nbPageR<ratioP1:
                    if key in H:
                        T.append((H[key],db["Y"].get(i),db["Z"].get(i)))
                        if len(T)==pageSize:
                            T=pd.DataFrame(T,columns=['X','Y','Z'])
                            T.to_csv('Data/'+folderName+"_hybrid_hash/T_"+str(iT)+".csv",sep=',',index=False)
                            iT+=1
                            T=[]
                else:
                    iP=((key%nbPageR)-ratioP1)//ratioP
                    buffers[iP].append((db["Y"].get(i),db["Z"].get(i)))
                    if len(buffers[iP])==pageSize:
                        Temp=pd.DataFrame(buffers[iP],columns=['Y','Z'])
                        Temp.to_csv('Data/'+folderName+"_hybrid_hash_partition/"+str(iP+1)+"/S_"+str(ibuffers[iP])+".csv",sep=',',index=False)
                        ibuffers[iP]+=1
                        buffers[iP]=[]

        for iP,P in enumerate(buffers):

            if P:
                Temp=pd.DataFrame(P,columns=['Y','Z'])
                Temp.to_csv('Data/'+folderName+"_hybrid_hash_partition/"+str(iP+1)+"/S_"+str(ibuffers[iP])+".csv",sep=',',index=False)

        timer+=time.time()-seconds

        for i in range(B-1):
            T,iT,timer2=simple_hash_join_file_loop(folderName+"_hybrid_hash_partition/"+str(i+1),memory,pageSize,T,iT,'Data/'+folderName+"_hybrid_hash")
            timer+=timer2
        seconds=time.time()
        if T:
            T=pd.DataFrame(T,columns=['X','Y','Z'])
            T.to_csv('Data/'+folderName+"_hybrid_hash/T_"+str(iT)+".csv",sep=',',index=False)

        return timer+(time.time()-seconds)

    elif math.ceil(nbPageR/(memory-1))<=memory-2:
        return (time.time()-seconds)+grace_hash_join_file(folderName,memory,pageSize)
    else:
        print("Pas définie")
        return 0.0
    


def grace_hash_join_file(folderName,memory,pageSize):

    assert memory>=3, "Erreur : La memoire doit contenir au moins 3 pages"

    if not os.path.exists('Data/'+folderName+"_grace_hash"):
        os.makedirs('Data/'+folderName+"_grace_hash")
    else:
        #vide le contenu
        delete_file("T",folderName+"_grace_hash")

    if not os.path.exists('Data/'+folderName+"_grace_hash_partition"):
        os.makedirs('Data/'+folderName+"_grace_hash_partition")
    else:
        #vide le contenu
        for f in os.listdir("Data/"+folderName+"_grace_hash_partition"):
            delete_folder(folderName+"_grace_hash_partition/"+f)


    nbPageR=len([f for f in os.listdir("Data/"+folderName) if ("R") in f])
    nbPageS=len([f for f in os.listdir("Data/"+folderName) if ("S") in f])

    assert math.ceil(nbPageR/(memory-1))<=memory-2, "Pas définie"

    nbPartition=min(memory-1,nbPageR)

    for i in range(nbPartition):
        os.makedirs('Data/'+folderName+"_grace_hash_partition/"+str(i))
        

    seconds=time.time()
    
    nbPartition=min(memory-1,nbPageR)

    buffers=[[] for _ in range(nbPartition)]
    ibuffers=[0 for _ in range(nbPartition)]
    for pageR in range(1,nbPageR+1):
        db=read_X_pages(folderName+"/R",pageR,1) 
        for i in range(len(db.index)):
            key=int(db["Y"].get(i))%nbPartition
            buffers[key].append((db["X"].get(i),db["Y"].get(i)))
            if len(buffers[key])==pageSize:
                Temp=pd.DataFrame(buffers[key],columns=['X','Y'])
                Temp.to_csv('Data/'+folderName+"_grace_hash_partition/"+str(key)+"/R_"+str(ibuffers[key])+".csv",sep=',',index=False)
                ibuffers[key]+=1
                buffers[key]=[]
                del Temp
    for key,B in enumerate(buffers):

        if B:
            Temp=pd.DataFrame(B,columns=['X','Y'])
            Temp.to_csv('Data/'+folderName+"_grace_hash_partition/"+str(key)+"/R_"+str(ibuffers[key])+".csv",sep=',',index=False)
            del Temp
        ibuffers[key]=0
        buffers[key]=[]

    for pageS in range(1,nbPageS+1):
        db=read_X_pages(folderName+"/S",pageS,1) 
        for i in range(len(db.index)):
            key=int(db["Y"].get(i))%nbPartition
            buffers[key].append((db["Y"].get(i),db["Z"].get(i)))
            if len(buffers[key])==pageSize:
                Temp=pd.DataFrame(buffers[key],columns=['Y','Z'])
                Temp.to_csv('Data/'+folderName+"_grace_hash_partition/"+str(key)+"/S_"+str(ibuffers[key])+".csv",sep=',',index=False)
                ibuffers[key]+=1
                buffers[key]=[]
                del Temp
    for key,B in enumerate(buffers):
        if B:
            Temp=pd.DataFrame(B,columns=['Y','Z'])
            Temp.to_csv('Data/'+folderName+"_grace_hash_partition/"+str(key)+"/S_"+str(ibuffers[key])+".csv",sep=',',index=False)
            del Temp
    del ibuffers
    del buffers
    iT=1
    T=[]

    timer=time.time()-seconds

    for i in range(nbPartition):
        T,iT,timer2=simple_hash_join_file_loop(folderName+"_grace_hash_partition/"+str(i),memory,pageSize,T,iT,'Data/'+folderName+"_grace_hash")
        timer+=timer2

    seconds=time.time()

    if T:
        T=pd.DataFrame(T,columns=['X','Y','Z'])
        T.to_csv('Data/'+folderName+"_grace_hash/T_"+str(iT)+".csv",sep=',',index=False)
    return timer+(time.time()-seconds)

def simple_hash_join_file_loop(folderName,memory,pageSize,T,iT,TPath):

    nbPageR=len([f for f in os.listdir("Data/"+folderName) if ("R") in f])
    nbPageS=len([f for f in os.listdir("Data/"+folderName) if ("S") in f])
    
    seconds=time.time()

    nbPartitions= math.ceil(nbPageR/(memory-2))
    assert nbPartitions==1, "Débordement de la mémoire"
    
    #R peut être hachee dans la memoire
    H=dict()
    for pageR in range(nbPageR):
        db=read_X_pages(folderName+"/R",pageR,1) 
        for i in range(len(db.index)):
            key=int(db["Y"].get(i))
            H[key]=db["X"].get(i)
    for i in range(nbPageS):
        db=read_X_pages(folderName+"/S",i,1)
        for j in range(len(db.index)):
            key=int(db["Y"].get(j))
            if key in H:
                T.append((H[key],db["Y"].get(j),db["Z"].get(j)))
                if len(T)==pageSize:
                    T=pd.DataFrame(T,columns=['X','Y','Z'])
                    T.to_csv(TPath+"/T_"+str(iT)+".csv",sep=',',index=False)
                    iT+=1
                    T=[]
    return T,iT,time.time()-seconds
    
        

def simple_hash_join_file(folderName,memory,pageSize):

    assert memory>=3, "Erreur : La memoire doit contenir au moins 3 pages"

    if not os.path.exists('Data/'+folderName+"_hash"):
        os.makedirs('Data/'+folderName+"_hash")
    else:
        #vide le contenu
        delete_file("T",folderName+"_hash")
    
    #métadonnées
    nbPageR=len([f for f in os.listdir("Data/"+folderName) if ("R") in f])
    nbPageS=len([f for f in os.listdir("Data/"+folderName) if ("S") in f])

    seconds=time.time()

    T=[]
    iT=1
    
    nbPartitions= math.ceil(nbPageR/(memory-2))
    
    #R peut être hachee dans la memoire
    if nbPartitions==1:
        H=dict()
        for pageR in range(1,nbPageR+1):
            db=read_X_pages(folderName+"/R",pageR,1) 
            for i in range(len(db.index)):
                key=int(db["Y"].get(i))
                H[key]=db["X"].get(i)
        for i in range(nbPageS):
            db=read_X_pages(folderName+"/S",i+1,1)
            for j in range(len(db.index)):
                key=int(db["Y"].get(j))
                if key in H:
                    T.append((H[key],db["Y"].get(j),db["Z"].get(j)))
                    if len(T)==pageSize:
                        T=pd.DataFrame(T,columns=['X','Y','Z'])
                        T.to_csv('Data/'+folderName+"_hash/T_"+str(iT)+".csv",sep=',',index=False)
                        iT+=1
                        T=[]
        if T:
            T=pd.DataFrame(T,columns=['X','Y','Z'])
            T.to_csv('Data/'+folderName+"_hash/T_"+str(iT)+".csv",sep=',',index=False)
        return time.time()-seconds
    #on divise R entre n table de taille M-2 de hachage 
    else:

        timer=time.time()-seconds

        if not os.path.exists('Data/'+folderName+"_hash_temp"):
            os.makedirs('Data/'+folderName+"_hash_temp")
        else:
            delete_file("csv",folderName+"_hash_temp")

        seconds=time.time()

        passe=0
        iPageR=1
        while passe<nbPartitions:
            H=dict()
            tempdb=[]
            iS=1
            iR=1
            path1="_hash_temp" if passe!=0 else ""
            path2=str(passe-1) if passe!=0 else  ""       
            #Parcours de R page par page
            for pageR in range(1,nbPageR+1):
                #SI passe 0 on lit R sinon R_temp
                
                
                db=read_X_pages(folderName+path1+"/R"+path2,pageR,1) 
                for i in range(len(db.index)):
                    #Si la cle appartient a la partition passe
                    key_p=int(db["Y"].get(i))
                    if key_p%nbPartitions==passe:
                        key=int(db["Y"].get(i))
                        H[key]=db["X"].get(i)
                    else:
                        tempdb.append((db["X"].get(i),db["Y"].get(i)))
                        #Temp_R plein
                        if len(tempdb)==pageSize:
                            
                            tempdb=pd.DataFrame(tempdb,columns=['X','Y'])
                            tempdb.to_csv('Data/'+folderName+"_hash_temp/R"+str(passe)+"_"+str(iR)+".csv",sep=',',index=False)
                            iR+=1
                            tempdb=[]

            if tempdb:
                tempdb=pd.DataFrame(tempdb,columns=['X','Y'])
                tempdb.to_csv('Data/'+folderName+"_hash_temp/R"+str(passe)+"_"+str(iR)+".csv",sep=',',index=False)
                tempdb=[]

             
            #Parcours de S page par page    
            for pageS in range(1,nbPageS+1):
                #SI partition 0 on lit R sinon R_temp
                db=read_X_pages(folderName+path1+"/S"+path2,pageS,1) 
                for j in range(len(db.index)):
                    key_p=int(db["Y"].get(j))
                    #Si la cle appartient a la partition passe
                    if key_p%nbPartitions==passe:
                        key=int(db["Y"].get(j))
                        #Si la cle appartient a la in memory table H
                        if key in H:
                            T.append((H[key],db["Y"].get(j),db["Z"].get(j)))
                            if len(T)==pageSize:
                                T=pd.DataFrame(T,columns=['X','Y','Z'])
                                T.to_csv('Data/'+folderName+"_hash/T_"+str(iT)+".csv",sep=',',index=False)
                                iT+=1
                                T=[]
                            
                    else:                        
                        tempdb.append((db["Y"].get(j),db["Z"].get(j)))
                        if len(tempdb)==pageSize:
                            tempdb=pd.DataFrame(tempdb,columns=['Y','Z'])
                            tempdb.to_csv('Data/'+folderName+"_hash_temp/S"+str(passe)+"_"+str(iS)+".csv",sep=',',index=False)
                            iS+=1
                            tempdb=[]
            if tempdb:
                tempdb=pd.DataFrame(tempdb,columns=['Y','Z'])
                tempdb.to_csv('Data/'+folderName+"_hash_temp/S"+str(passe)+"_"+str(iS)+".csv",sep=',',index=False)
                tempdb=[]
                
            timer+=time.time()-seconds

            nbPageR=len([f for f in os.listdir("Data/"+folderName+"_hash_temp") if "R"+str(passe) in f])
            nbPageS=len([f for f in os.listdir("Data/"+folderName+"_hash_temp") if "S"+str(passe) in f])

            seconds=time.time()

            passe+=1

        if T:
            T=pd.DataFrame(T,columns=['X','Y','Z'])
            T.to_csv('Data/'+folderName+"_hash/T_"+str(iT)+".csv",sep=',',index=False)
        return timer+(time.time()-seconds)



def simple_hash_join_cost(nbTuplesR,nbTuplesS,selectivity,memory,pageSize):
    '''Retourne le nombre de lectures,ecritures et le cout en temps(ms) du simple_hash_join'''
    
    #metadonnées
    R_pages=math.ceil(nbTuplesR/pageSize)
    S_pages=math.ceil(nbTuplesS/pageSize)
    
    
    #Initialisation
    read= R_pages+S_pages 
    write= math.ceil(R_pages*selectivity)
    cost=math.ceil(nbTuplesR*selectivity)*MOVE
    cost_build=0
    #H rentre dans la mémoire
    if R_pages<= memory-2:
        cost+= nbTuplesR * (HASH+MOVE) + nbTuplesS* (HASH+COMP) + (read*IO_READ+write*IO_WRITE)
        return read,write,cost_build,cost/1000
    else:
    #H ne pas rentre dans la mémoire
        B= math.ceil(R_pages/(memory-3))
        Ar=math.ceil(R_pages/B)
        As=math.ceil(S_pages/B)
        tmp=(B*(B-1)/2)*(Ar+As)
        read+= tmp 
        write+=tmp
        cost += (B+1)/2*(nbTuplesR+nbTuplesS)*(HASH+MOVE) - nbTuplesS*MOVE + nbTuplesR*HASH + nbTuplesS*(COMP+HASH) + (read*IO_READ+write*IO_WRITE)
   
    return read, write, cost_build, cost/1000

def grace_hash_join_cost(nbTuplesR,nbTuplesS,selectivity,memory,pageSize):
    '''Retourne le nombre de lectures,ecritures et le cout en temps(ms) du grace_hash_join'''
    
    #metadonnées
    R_pages=math.ceil(nbTuplesR/pageSize)
    S_pages=math.ceil(nbTuplesS/pageSize)
    
    

    B=memory-1 if memory-1 < R_pages else R_pages
    #Grace non defini
    if math.ceil(R_pages/B) > memory-2:
        print("Non définie")
        return 0,0,0,0
   
    Ar=math.ceil(R_pages/B)
    As=math.ceil(S_pages/B)
    #BUILD
    read_build= R_pages+S_pages 
    write_build= B *(As+Ar)
    cost_build= (read_build*IO_READ+write_build*IO_WRITE) + (nbTuplesR+nbTuplesS)*(HASH+MOVE)
    #PROBE
    read_probe= B *(As+Ar)
    write_probe=  math.ceil(R_pages*selectivity)
    cost_probe= math.ceil(nbTuplesR*selectivity)*MOVE + nbTuplesR*(HASH+MOVE) + nbTuplesS*(HASH+COMP) + (read_probe*IO_READ+write_probe*IO_WRITE) 
    
    
    return read_build+read_probe,write_build+write_probe,cost_build/1000,cost_probe/1000

def hybrid_hash_join_cost(nbTuplesR,nbTuplesS,selectivity,memory,pageSize):
    '''Retourne le nombre de lectures,ecritures et le cout en temps(ms) du hybride_hash_join'''
    
    #metadonnées
    R_pages=math.ceil(nbTuplesR/pageSize)
    S_pages=math.ceil(nbTuplesS/pageSize)
    
    
    cost_build=0
    B= 1+ math.ceil((R_pages-memory+2)/(memory-3))
   
    #Equivalent au simple hash join
    if R_pages<=memory-2:
        return simple_hash_join_cost(nbTuplesR,nbTuplesS,selectivity,memory,pageSize)
    
    #Grace ou non defini
    if B> memory-2:
        return grace_hash_join_cost(nbTuplesR,nbTuplesS,selectivity,memory,pageSize)
    #Hybride

    Ar_0 = memory-2-(B-1)  
    As_0 = int(Ar_0/R_pages*S_pages)
    Ar=math.ceil((R_pages-Ar_0)/(B-1))
    As=math.ceil((S_pages-As_0)/(B-1))
    
    read= R_pages+S_pages + R_pages-Ar_0 + S_pages-As_0
    write=  math.ceil(R_pages*selectivity)+ R_pages-Ar_0 + S_pages-As_0
    cost=(read*IO_READ+write*IO_WRITE) + math.ceil(nbTuplesR*selectivity)*MOVE
    
    cost+= (nbTuplesR+nbTuplesS)*(HASH+MOVE) - As_0*MOVE + nbTuplesR*HASH + nbTuplesS*(HASH+COMP)
    
    return read,write,cost_build,cost/1000
        


