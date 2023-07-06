from tools import *


def hash_join_file(folderName,memory,pageSize):

    if not os.path.exists('Data/'+folderName+"_hash"):
        os.makedirs('Data/'+folderName+"_hash")
    else:
        #vide le contenu
        delete_file("T",folderName+"_hash",)

    
    nbPageR=len([f for f in os.listdir("Data/"+folderName) if ("R") in f])
    nbPartition=math.ceil(nbPageR/(memory-2))
    partition=0
    dic=dict()
    R_temp=[]
    iR=1
    for i in range(nbPageR):
        db=read_X_pages(folderName+"/R",i+1,1)
        for j in range(len(db.index)):
            key=int(db["Y"].get(j))%nbPartition
            if key==partition:
                if key in dic:
                    dic[key].append(db["Y"].get(j))
                else:
                    dic[key]=[db["Y"].get(j)]
            else:
                R_temp.append((db["X"].get(j),db["Y"].get(j)))
                if len(R_temp)==pageSize:
                    R_temp=pd.DataFrame(R_temp,columns=['X','Y'])
                    R_temp.to_csv('Data/'+folderName+"_hash/R_"+str(iR)+".csv",sep=',',index=False)
                    iR+=1
                    R_temp=[]
    if R_temp:
        R_temp=pd.DataFrame(R_temp,columns=['X','Y'])
        R_temp.to_csv('Data/'+folderName+"_hash/R_"+str(iR)+".csv",sep=',',index=False)
    print(dic)




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
        


