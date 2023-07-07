from tools import *


def simple_hash_join_file(folderName,memory,pageSize):

    assert memory>=3, "Erreur : La memoire doit contenir au moins 3 pages"

    if not os.path.exists('Data/'+folderName+"_hash"):
        os.makedirs('Data/'+folderName+"_hash")
    else:
        #vide le contenu
        delete_file("T",folderName+"_hash")
    
    
    nbPageR=len([f for f in os.listdir("Data/"+folderName) if ("R") in f])
    nbPageS=len([f for f in os.listdir("Data/"+folderName) if ("S") in f])
    T=[]
    iT=1

    if nbPageR<=memory-2:
        H=dict()
        db=read_X_pages(folderName+"/R",1,nbPageR)
        for i in range(len(db.index)):
            key=hash(int(db["Y"].get(i)))
            H[key]=db["X"].get(i)
        for i in range(nbPageS):
            db=read_X_pages(folderName+"/S",i+1,1)
            for j in range(len(db.index)):
                key=hash(int(db["Y"].get(j)))
                if key in H:
                    T.append((H[key],db["Y"].get(j),db["Z"].get(j)))
                    if len(T)==pageSize:
                        print(T)
                        T=pd.DataFrame(T,columns=['X','Y','Z'])
                        T.to_csv('Data/'+folderName+"_hash/T_"+str(iT)+".csv",sep=',',index=False)
                        iT+=1
                        T=[]
        if T:
            T=pd.DataFrame(T,columns=['X','Y','Z'])
            T.to_csv('Data/'+folderName+"_hash/T_"+str(iT)+".csv",sep=',',index=False)

    else:

        if not os.path.exists('Data/'+folderName+"_hash_temp"):
            os.makedirs('Data/'+folderName+"_hash_temp")
        else:
            #vide le contenu
            delete_file("S",folderName+"_hash_temp")

        iPageR=1
        tempString=""
        while iPageR<=nbPageR:
            H=dict()
            tempdb=[]
            iS=1
            for i in range((memory-2)):
                if iPageR<=nbPageR:
                    db=read_X_pages(folderName+"/R",iPageR,1)
                    for j in range(len(db.index)):
                        key=hash(int(db["Y"].get(j)))
                        assert key not in H, "Colision"
                        H[key]=db["X"].get(j)
                    iPageR+=1
            for i in range(nbPageS):
                db=read_X_pages(folderName+tempString+"/S",i+1,1)
                for j in range(len(db.index)):
                    key=hash(int(db["Y"].get(j)))
                    if key in H:
                        T.append((H[key],db["Y"].get(j),db["Z"].get(j)))
                        if len(T)==pageSize:
                            print(T)
                            T=pd.DataFrame(T,columns=['X','Y','Z'])
                            T.to_csv('Data/'+folderName+"_hash/T_"+str(iT)+".csv",sep=',',index=False)
                            iT+=1
                            T=[]
                    else:
                        tempdb.append((db["Y"].get(j),db["Z"].get(j)))
                        if len(tempdb)==pageSize:
                            tempdb=pd.DataFrame(tempdb,columns=['Y','Z'])
                            delete_file("S_"+str(iS)+".csv",folderName+"_hash_temp")
                            tempdb.to_csv('Data/'+folderName+"_hash_temp/S_"+str(iS)+".csv",sep=',',index=False)
                            iS+=1
                            tempdb=[]
            if tempdb:
                tempdb=pd.DataFrame(tempdb,columns=['Y','Z'])
                tempdb.to_csv('Data/'+folderName+"_hash_temp/S_"+str(iS)+".csv",sep=',',index=False)
                iS+=1
            tempString="_hash_temp"
            nbPageS=iS-1
        if T:
            T=pd.DataFrame(T,columns=['X','Y','Z'])
            T.to_csv('Data/'+folderName+"_hash/T_"+str(iT)+".csv",sep=',',index=False)




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
        


