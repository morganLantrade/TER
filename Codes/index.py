#######################################################################
# source : https://www.geeksforgeeks.org/introduction-of-b-tree-2/
# modifié pour notre structure de données et le transformer en B+ tree
########################################################################
import pandas as pd
from tools import *


class BTreeNode:
  '''Noeud de l'arbre'''
  def __init__(self, leaf=False):
    self.leaf = leaf
    self.keys = []
    self.child = []
 
 
class BTree:
  '''order correspont au degres de l'arbre, max feuille = order - 1'''
  def __init__(self, order):
    self.root = BTreeNode(True)
    self.order = order
 
  
  def insert(self, k):
    '''Insertion de k dans l'arbre'''
    root = self.root
    #arbre plein
    if len(root.keys) == self.order - 1:
      temp = BTreeNode()
      self.root = temp
      temp.child.insert(0, root)
      self.split_child(temp, 0)
      self.insert_non_full(temp, k)

    else:
      self.insert_non_full(root, k)
 

  def insert_non_full(self, x, k):
    '''Insertion dans un noeud non plein'''
    i = len(x.keys) - 1
    if x.leaf:
      x.keys.append((None, None))
      #insertion au milieu d'un noeud
      while i >= 0 and k[0] < x.keys[i][0]:
        x.keys[i + 1] = x.keys[i]
        i -= 1
      x.keys[i + 1] = k
    else:
      while i >= 0 and k[0] < x.keys[i][0]:
        i -= 1
      i += 1
      #si le children est plein
      if  len(x.child[i].keys) == self.order :
        self.split_child(x, i)
        if k[0] > x.keys[i][0]:
          i += 1
      #insertion a la fin 
      self.insert_non_full(x.child[i], k)
 
  def split_child(self, x, i):
    '''Debordement, separation du children'''
    y = x.child[i]
    z = BTreeNode(y.leaf)
    x.child.insert(i+1 , z)
    x.keys.insert(i, y.keys[self.order//2 - 1])
    #si l'enfant de x possede des feuilles
    if y.leaf:
      z.keys = [y.keys[self.order//2-1]]+y.keys[self.order//2:] 
      y.keys =  y.keys[:self.order//2-1]
    else:
      z.child =  y.child[self.order//2:]
      y.child = y.child[:self.order//2]
      z.keys = y.keys[self.order//2:] 
      y.keys =  y.keys[:self.order//2-1]
    
 
  
  def print_tree(self, x, l=0):
    '''Affiche l'arbre en longueur'''
    print("Level ", l, " ( ", end=" ")
    for i in x.keys:
      print(i, end=" ")
    print(")")
    l += 1
    if len(x.child) > 0:
      for i in x.child:
        self.print_tree(i, l)

  def levels(self,x,dic,l=0):
    '''Renvoie un dictionnaire ou les cles sont les niveaux et
     les valeurs sont les différents branche ou feuilles correspondants  '''  
    if l in dic:
      dic[l].append([ k for k in x.keys] )
    else: 
      dic[l]=[[ k for k in x.keys]]
    l += 1
    if len(x.child) > 0:
      for i in x.child:
        self.levels(i,dic, l)
    return dic     
 
def index_of(dataframe,pageSize):
    '''Renvoie le dictionnaire levels selon un dataframe et la taille d'une page(en tuple)'''
    B=BTree(pageSize)
    L= dataframe.values.tolist()
    for y,x in L:
        B.insert((int(y),int(x)))
    B.print_tree(B.root)
    return B.levels(B.root,dict())

def index_page(dic):
    '''Retourne une liste de liste correspondant aux differente pas a ecrire avec leur contenu et le nombre de niveaux'''
    cpt_page=1
    cpt_page_level=0 
    pages=[]
    #Niveau pointeur
    for level in list(dic.keys())[:-1]:
        for v in dic[level]:
            page=[]
            for p in v:
                page.append((cpt_page,p[0]))
                cpt_page+=1
            page.append((cpt_page,-1))
            cpt_page+=1
            pages.append(page)
        cpt_page_level+=1 

    #Niveau feuille
    for v in dic[len(list(dic.keys()))-1]:
        page=[]
        for p in v:
            page.append((p[1],p[0]))
        pages.append(page)
    return pages,cpt_page_level

def index_to_file(folderName,pageSize,dbName):
    '''Ecrit dans la run foldername_idx, l'index et retourne le nombre de niveaux'''
    if not os.path.exists('Data/'+folderName+"_idx"):
        os.makedirs('Data/'+folderName+"_idx")
    else:
        #vide le contenu
        delete_file("I",folderName+"_idx")
    #metadonnee
    nbPage=len([f for f in os.listdir("Data/"+folderName) if dbName in f])

    #chargement du dataframe
    db=read_X_pages(folderName+"/"+dbName,1,nbPage)
    dic = index_of(db,pageSize)
    print(dic)
    pages,lvl = index_page(dic)

    for i,p in enumerate(pages):
        T=p
        T=pd.DataFrame(T,columns=['Y','Z'])
        T.to_csv('Data/'+folderName+"_idx/"+"I_"+str(i)+".csv",sep=',',index=False)
    return lvl

def search_in_page(folderName,num_page,key,i,level,pageSize):
    '''Renvoie la page du prochain niveau ou Z si le level est le dernier'''
    cpt=0
    with open('Data/'+folderName+"_idx/I_"+str(num_page)+".csv", 'r') as file:
        spamreader = csv.reader(file, delimiter=',', quotechar='|')
        a=next(spamreader)
        #Tant que la key n'est pas supperieure et qu'il existe des lignes
        for y in spamreader:
            page=int(y[0])
            if int(y[1])> key and level!=i or int(y[1])==key and level==i:
                return page

            
        return page if level!=i else None
       

def search_index(folderName,level,key,pageSize,root=None):
    '''Retourne le tuple si il existe dans l'index de la run folderName None sinon'''
    lvl=1
    l=0
    #La racine est deja chargee en memoire
    if root is not None:
        while l<len(root.index) and int(root["Z"].get(l))<key:
            l+=1
        next_page=int(root["Y"].get(l-1))
    else:
        next_page=search_in_page(folderName,0,key,lvl,level,pageSize)
    #on parcour les niveaux
    while lvl<=level:
        next_page=search_in_page(folderName,next_page,key,lvl,level,pageSize)
        lvl+=1
    return next_page
        
   
    
    




def main():
    pass
    
  
  
                
      
            



 
 
if __name__ == '__main__':
  main()