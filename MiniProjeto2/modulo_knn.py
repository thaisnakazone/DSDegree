class ClassificaPerfil:
    dicio_perfil_classificado = {}
    '''
    Criou-se um dicionário vazio, como atributo estático, que armazenará, a cada iteração do comando "for" (no arquivo MiniProjeto2.ipynb), 
    o resultado do conjunto de instruções da classe "ClassificaPerfil" (ou seja, esse dicionário armazenará a classificação do perfil dos
    clientes com carteiras não treinadas).
    '''
    
    
    def __init__(self, data, no_class, k, ind_no_class):
        '''
        A classe "ClassificaPerfil" recebe os seguintes atributos:
        - uma lista com 120 clientes cujas carteiras já foram treinadas;
        - uma lista com 30 clientes a serem classificados, de acordo com sua carteira ainda não treinada;
        - o valor de k (que define a quantidade de vizinhos a ser utilizada para classificar um perfil);
        - o índice que identifica cada elemento da lista com os 30 clientes a serem classificados.
        Além desses atributos, criou-se:
        - uma lista vazia que armazenará o resultado do conjunto de instruções do método "calcula_dist";
        - um dicionário vazio que armazenará o resultado do conjunto de instruções do método "freq_perfil_menores_dist".
        '''
        self.data = data
        self.no_class = no_class
        self.k = k
        self.ind_no_class = ind_no_class
        self.lista_dist_ord = []
        self.dicio_freq_perfil = {}
        
    def calcula_dist(self):
        '''
        Este método recebe os seguintes argumentos (self), já definidos no método construtor:
        - a lista com os 120 clientes cujas carteiras já foram treinadas;
        - a lista com os 30 clientes a serem classificados, de acordo com sua carteira ainda não treinada;
        - o índice que identifica cada elemento da lista com os 30 clientes a serem classificados.
        Esse método calcula a distância euclidiana entre dois pontos (uma carteira treinada e outra não treinada).
        E depois, retorna uma lista com 120 distâncias, ordenadas da mais próxima à mais distante,
        que foram obtidas comparando-se uma carteira não treinada com as outras 120 já treinadas.
        '''
        lista_dist = []
        lista_perf_data = []
        for ind_data in range(len(self.data)):
            lista_subtrai = list(zip(self.no_class[self.ind_no_class][2], self.data[ind_data][2]))
            lista_perf_data.append(self.data[ind_data][1])
            for indice in range(len(lista_subtrai)):
                lista_subtrai[indice] = (lista_subtrai[indice][1] - lista_subtrai[indice][0]) ** 2
            distancia = (sum(lista_subtrai) ** 0.5)
            lista_dist.append(distancia)
            lista_dist_perf = list(zip(lista_dist, lista_perf_data))
            self.lista_dist_ord = sorted(lista_dist_perf)
        return self.lista_dist_ord

    def freq_perfil_menores_dist(self):
        '''
        Este método recebe os seguintes argumentos (self), já definidos no método construtor:
        - o valor de k (que define a quantidade de vizinhos a ser utilizada para classificar um perfil);
        - o retorno do método anterior (a lista com 120 distâncias).
        Esse método cria uma lista com o perfil dos 5 (k = 5) clientes (com carteiras treinadas) que obtiveram as distâncias mais próximas,
        em relação a um cliente com uma carteira não treinada. Depois, verifica-se a frequência de cada perfil nessa lista.
        Por fim, esse método retorna um dicionário, em que a chave é o perfil do cliente (com carteira treinada) e o valor é a quantidade 
        de vezes que esse perfil aparece na lista criada anteriormente.
        '''
        lista_perfil_menores_dist = []
        for menores_distancias, perfil in self.lista_dist_ord[:self.k]:
            if perfil == 'Conservador':
                perfil_no_class = perfil
                lista_perfil_menores_dist.append(perfil_no_class)
            elif perfil == 'Moderado':
                perfil_no_class = perfil
                lista_perfil_menores_dist.append(perfil_no_class)
            elif perfil == 'Agressivo':
                perfil_no_class = perfil
                lista_perfil_menores_dist.append(perfil_no_class)
        
        for perfil_no_class in lista_perfil_menores_dist:
            if (perfil_no_class in self.dicio_freq_perfil):
                self.dicio_freq_perfil[perfil_no_class] += 1
            else:
                self.dicio_freq_perfil[perfil_no_class] = 1
        return self.dicio_freq_perfil

    def classifica_perfil(self):
        '''
        Este método recebe os seguintes argumentos (self), já definidos no método construtor:
        - a lista com os 30 clientes a serem classificados, de acordo com sua carteira ainda não treinada;
        - o valor de k (que define a quantidade de vizinhos a ser utilizada para classificar um perfil);
        - o índice que identifica cada elemento da lista com os 30 clientes a serem classificados;
        - o retorno do método anterior (dicionário com a frequência do perfil dos 5 clientes com as distâncias mais próximas).
        Esse método busca (no dicionário do método anterior) o perfil que obteve uma frequência maior do que a metade do valor de k 
        (ou seja, o perfil que obteve a maior frequência na lista dos 5 clientes com as distâncias mais próximas) e, então, 
        classifica o cliente com uma carteira não treinada, armazenando essas informações em um dicionário.
        Depois, retorna-se esse dicionário, em que a chave é o CPF do cliente e o valor é o perfil classificado.
        '''
        for chave, valor in self.dicio_freq_perfil.items():
            if valor > (self.k / 2):
                ClassificaPerfil.dicio_perfil_classificado[self.no_class[self.ind_no_class][0]] = chave
        return ClassificaPerfil.dicio_perfil_classificado
    
    @staticmethod
    def mostra_classificacao():
        '''
        Este método estático não recebe nenhum argumento, pois o dicionário (que deve mostrar os perfis classificados) 
        foi criado como um atributo estático da classe "ClassificaPerfil".
        Por fim, retorna-se o dicionário com os 30 perfis classificados.
        '''
        return ClassificaPerfil.dicio_perfil_classificado