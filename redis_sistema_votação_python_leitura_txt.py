import redis

r = redis.StrictRedis(host='localhost', port=6379, charset="utf-8", decode_responses=True, db=0)

r.set('candidato1', 0)
r.set('candidato2', 0)

file = open('votação.txt', 'r') 
voto = file.readlines() 

for l in voto :
    k = l.replace('\n', '')
    if k == 'candidato1':
       r.incr('candidato1')
    else:
       r.incr('candidato2')
       
candidato1 = r.get('candidato1')
candidato2 = r.get('candidato2')

if candidato1 > candidato2:
   print('candidato 1 venceu a eleição')
else:
   print('candidato 2 venceu a eleição')        