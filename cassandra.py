from cassandra.cluster import Cluster 
cluster = Cluster()
session = cluster.connect('Aula')

def criarBD():
    session.execute("CREATE KEYSPACE Aula WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '3'}")
def criarplaylist_versionada():
    session.execute("CREATE TABLE playlist_versionada ( id_playlist int, versao int, modificacao text,PRIMARY KEY (id_playlist, versao)) WITH COMPACT STORAGE ")
def criartabelaPlaylistAtual():
    session.execute("CREATE TABLE playlist_atual ( id_playlist int, posicao int, id_musica uuid, nome text, album text, artista text,PRIMARY KEY (id_playlist, posicao))")
def inserirplaylist_versionada(idplaylist,versao,modificacao):
    session.execute("INSERT INTO playlist_versionada (id_playlist, versao, modificacao)VALUES("+idplaylist+","+versao+","+modificacao+")")
def playlist_atual(id_playlist, posicao, id_musica, nome, album, artista):
    session.execute("INSERT INTO playlist_versionada (id_playlist, posicao, id_musica, nome, album, artist)VALUES("+id_playlist+","+posicao+","+id_musica+","+nome+","+album+","+artista+")")
def atualizarPlaylistAtual(id_playlist,id_musica,nome,album,artista):
    result = session.execute("select * from playlist_versionada WHERE id="+id_playlist+")")
    acao1 = (result.modificacao)[-1]
    id_playlist=(result.id_playlist)[-1]
    posicao = (result.posicao)[-1]
    acao = acao1[1] 
    if acao =='a':
        playlist_atual(id_playlist, posicao, id_musica, nome, album, artista)
    if acao == 't':
        session.execute("UPDATE playlist_atual SET posicao="+posicao+"WHERE id="+id_playlist+")")
    if acao == 'd':
        session.execute("DELETE playlist_atual WHERE id="+id_playlist+")")
def imprimir():
       r = session.execute("SELECT * From playlist_atual")
       for i in r:
           print(r.nome)
           print(r.artista)
           print(r.posicao)

if __name__ == "__main__":
    criarBD()
    criarplaylist_versionada()
    criartabelaPlaylistAtual(1, 1, '62d77d6f-d6d7-46ca-a1a1-6476f1914812', 'Help!', 'Help!', 'Beatles')
    criartabelaPlaylistAtual(1, 2, '26203678-0fa0-43a3-a02d-4c3f10ab66f5', 'Yesterday', 'Help!', 'Beatles')
    criartabelaPlaylistAtual(1, 3, 'eca2f815-d29a-424f-a0d0-4682d356521d', 'Something', 'Abbey Road', 'Beatles')
    criartabelaPlaylistAtual(1, 4, '54665e70-32d8-46e0-8673-9fed563e872a', 'Blackbird', 'The Beatles', 'Beatles')
    inserirplaylist_versionada(1, 3, 'adiciona(Blackbird)')        
    inserirplaylist_versionada(1, 2, 'adiciona(Yesterday)') 
    inserirplaylist_versionada(1, 1, 'adiciona(Help!)')
    inserirplaylist_versionada(1, 1, 'adiciona(Help!)')
    atualizarPlaylistAtual(1,'62d77d6f-d6d7-46ca-a1a1-6476f1914812','Help','Help','Beatles')
    imprimir()