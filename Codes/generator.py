import random

def generate_triplets(N, C):
    triplets = []
    
    # Générer les C triplets de la forme x = y = z
    for _ in range(C):
        value = random.randint(1, N)
        triplets.append((value, value, value))
    
    # Générer les autres triplets avec x != y et y != z
    for _ in range(N - C):
        x = random.randint(1, N)
        y = random.randint(1, N)
        while y == x:
            y = random.randint(1, N)
        z = random.randint(1, N)
        while z == y:
            z = random.randint(1, N)
        triplets.append((x, y, z))
    
    return triplets

# Exemple d'utilisation avec N = 10 et C = 3
N = 10
C = 3
triplets = generate_triplets(N, C)

# Afficher les triplets générés
for triplet in triplets:
    print(triplet)