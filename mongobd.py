from pymongo import MongoClient

def clientebd():
    cliente = MongoClient('localhost', 27017)   
    db = cliente.artistas
    return db

def inserirArtista(nome,datanacimento,nacionalidade):
    db = clientebd()
    db.artistas.insert({"Nome":nome,"Data de Nacimento":datanacimento,"Nacionalidade":nacionalidade})
   
def inseriralbum(idartista, nomeartista, nomealbum):    
    db = clientebd()
    db.abuns.insert({"Id Artista":idartista,"Nome Artista":nomeartista,"Nome Album":nomealbum})

def buscarId(nomeartista):
    db = clientebd()
    artistasid=db.artistas.find_one({"Nome":nomeartista})
    idartista = artistasid["_id"]
    return idartista

def imprimirAlbunsComMaiorFrenquencia(freq):
     db = clientebd()
     artista_freq = db.artistas.find({"Nacionalidade":freq})
     for item in artista_freq:
         nome = item['Nome']
         listalbum = db.abuns.find({"Nome Artista":nome})
         for item2 in listalbum:
             print(str("Nome arstista "+ item2['Nome Artista']) +" Album " + str(item2['Nome Album']) + " Id artista "+ str(item2['Id Artista']) )

def BuscaNacionalidadeFrequentesComMongo():
    listafreq = []
    pipeline = [{'$group': {'_id': '$Nacionalidade', 'count': {'$sum': 1}}},{'$sort': {'count': -1},}]
    db = clientebd()
    a = list(db.artistas.aggregate(pipeline))
    for i in a:
        listafreq.append(i['_id'])
    return listafreq[0]    
   
    
def imprimirtodosArtistas():
    db = clientebd()
    artistas_lista = db.artistas.find({})
    for item in artistas_lista:
        print(item["Nome"] + item["Nacionalidade"] + item["Data de Nacimento"])

if __name__ == "__main__":
  inserirArtista("Rosa de Saron","11/10/2011","Brasileiro")
  Id = buscarId("Rosa de Saron")
  inseriralbum(Id,"Rosa de Saron","Horizonte Vivo Distante")
  
  inserirArtista("CMP22","3/3/2013","Brasileiro")
  Id = buscarId("CMP22")
  inseriralbum(Id,"CMP22","Acústico")
  
  inserirArtista("Jota Quest","3/2/2003","Brasileiro")
  Id = buscarId("Jota Quest")
  inseriralbum(Id,"Jota Quest","MTV ao vivo")
  
  inserirArtista("Michael Jackson","3/2/1987","Americano")
  Id = buscarId("Michael Jackson")
  inseriralbum(Id,"Michael Jackson","Thriller")
  
  
  nacionalidadefreq = BuscaNacionalidadeFrequentesComMongo()
  print(nacionalidadefreq)
  imprimirAlbunsComMaiorFrenquencia(nacionalidadefreq)
  