import random

# Create a node
class BTreeNode:
  def __init__(self, leaf=False):
    self.leaf = leaf
    self.keys = []
    self.child = []
 
 
# Tree
class BTree:
  def __init__(self, order):
    self.root = BTreeNode(True)
    self.order = order
 
    # Insert node
  def insert(self, k):
    root = self.root
    if len(root.keys) == self.order - 1:
      temp = BTreeNode()
      self.root = temp
      temp.child.insert(0, root)
      
      self.split_child(temp, 0)
      self.insert_non_full(temp, k)

    else:
      self.insert_non_full(root, k)
 
    # Insert nonfull
  def insert_non_full(self, x, k):
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
 
    # Split the child
  def split_child(self, x, i):
    y = x.child[i]
    z = BTreeNode(y.leaf)
    x.child.insert(i+1 , z)
    x.keys.insert(i, y.keys[self.order//2 - 1])
    
    if y.leaf:
      z.keys = [y.keys[self.order//2-1]]+y.keys[self.order//2:] #y.keys[self.order//2:]  #
      y.keys =  y.keys[:self.order//2-1]
    else:
      z.child =  y.child[self.order//2:]#y.child[self.order//2:]
      y.child = y.child[:self.order//2]
      z.keys = y.keys[self.order//2:] #y.keys[self.order//2:]  #
      y.keys =  y.keys[:self.order//2-1]
    
 
  # Print the tree
  def print_tree(self, x, l=0):
    print("Level ", l, " ( ", end=":")
    for i in x.keys:
      print(i, end=" ")

    print(")")
    l += 1
    if len(x.child) > 0:
      for i in x.child:
        self.print_tree(i, l)

  def levels(self,x,dic,l=0):
    print("Level ", l, " ", len(x.keys),len(x.child), end=":")
    
    for i in x.keys:
      print(i, end=" ")
    if l in dic:
      dic[l].append([ k for k in x.keys] )
    else: 
      dic[l]=[[ k for k in x.keys]]
    print()
    l += 1
    if len(x.child) > 0:
      for i in x.child:
        self.levels(i,dic, l)
    return dic     
 
  # Search key in the tree
  def search_key(self, k, x=None):
    if x is not None:
      i = 0
      while i < len(x.keys) and k> x.keys[i][0]:
        i += 1

      if i < len(x.keys) and k == x.keys[i][0]:
        return (x, i)
      elif x.leaf:
        return None
      else:
        
        return self.search_key(k, x.child[i])
       
    else:
      return self.search_key(k, self.root)


    

 
 
def main():
  B = BTree(6)
  L= [ i for i in range(100)]
  random.shuffle(L)
  for x in L:
    B.insert((x,0))
    if x<20:
      B.print_tree(B.root)
  dic=B.levels(B.root,dict())
  print(dic)
  cpt_page=1
  cpt_page_level=0 
  for level in list(dic.keys())[:-1]:
    print(level)
    print("-------------")
    for v in dic[level]:
      print("--",cpt_page_level,"--")
      cpt_page_level+=1 
      for p in v:
        print((cpt_page,p[0]))
        cpt_page+=1
      
      print((cpt_page,-1))
      cpt_page+=1
  print(level)
  print("-------------")
  for v in dic[len(list(dic.keys()))-1]:
        print("--",cpt_page_level,"--")
        cpt_page_level+=1 
        for p in v:
          print(p[1],p[0])
    
    
  
  
                
      
            


 
  if B.search_key(47) is not None:
    print("\nFound")
  else:
    print("\nNot Found")
 
 
if __name__ == '__main__':
  main()