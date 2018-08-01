import redis
from neo4jrestclient.client import GraphDatabase
from neo4jrestclient import client
r = redis.StrictRedis(host='localhost', port=6379, charset="utf-8", decode_responses=True, db=0)

C1 = ['ProdutoA', 'ProdutoB','ProdutoC','ProdutoD']
C2 = ['ProdutoA', 'ProdutoB']
C3 = ['ProdutoA', 'ProdutoB','ProdutoC']
C4 = ['ProdutoA', 'ProdutoB']

r.set('C1', C1)
r.set('C2', C2)
r.set('C3', C3)
r.set('C4', C4)

chaves = r.keys()


db = GraphDatabase("http://localhost:7474", username="neo4j", password="1234")
cliente = db.labels.create("Clientes")
produtosCliente = db.labels.create("ProdutosClientes")

for i in chaves:
   produtos = r.get(i)
   a = produtos[1:-1]
   a = a.split("', '")
   a[0] = a[0][1:]
   a[-1] = a[-1][:-1]
   print(type(a))
   c = db.nodes.create(name=i) 
   cliente.add(c)
   for j in a:
             print("lista produtos "+j)
             q = 'Merge(C:ProdutosClientes{name:"'+j+'"})'
             db.query(q)
             t = 'MATCH(C:Clientes),(P:ProdutosClientes) WHERE C.name="'+i+'"AND P.name ="'+j+'"CREATE (C)-[R:comprou]->(P) RETURN C,R,P'
             db.query(t)
b = 'MATCH (u:Clientes)-[:comprou]->(c1:ProdutosClientes), (u)-[:comprou]->(c2:ProdutosClientes)WHERE c1.name = "ProdutoA" AND c2.name = "ProdutoB" RETURN u'
results = db.query(b, returns=(client.Node))
i=0
for r in results:
    i = i + 1
porcentagem =str((i/4)*100)
print(porcentagem + " das pessoas que comprou o produto A, também compraram o produto B")

