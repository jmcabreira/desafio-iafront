# Desafio

Este é o desafio de contratação do time ia-front da B2W. Fique atento às regras e as considerações. **Você deve fazer um fork deste repositório**, comitar nele suas alterações e enviar o link quando estiver concluído.

Neste exercício vamos medir a sua capacidade de analisar e descrever o comportamento de visitas e venda a um e-commerce de acordo com grupos semânticos gerados a a partir de clusterizações. Apesar de não ser uma boa prática, vamos precisar que comite seus resultados (gráficos e datsets) no seu novo repositório, para que possamos analisá-lo antes da entrevista técnica onde será questionado sobre seu código e seu resultado. Uma boa prática é não misturar com o código, então mantenha seus dados numa pasta separada do módulo para evitar qualquer confusão e facilitar um possível git ignore posteriormente.

Ao longo do exercício são pedidas alguma análises e avaliações, você deve escrevê-las num arquivo **ANALISES.md** que deve ser posto na raiz do repositório. Se sinta livre para usar gráficos, tabelas, e tudo mais que achar relevante nesse arquivo para explicar o que foi questionado. Como estamos avaliando sua capacidade analítica, quanto mais matemáticos forem seus argumentos, melhor.

## Regras

Você deve criar aplicações estruturadas na forma de pipelines que recebem alguns parâmetros como entrada e têm como saída o artefato resultante do job em questão, que pode ser um ou mais conjuntos de dados, gráficos ou um relatório. Você será avaliado pela sua capacidade analítica e compreensão, mas também pela organização de seu código e correto uso das ferramentas selecionadas, isto é, você deve utilizar apenas as dependências incluídas no `setup.py` do arquivo.

Os jobs executados aqui, e que você vai criar, processarão um grande volume de dados, portanto tenha em mente que vale a pena trabalhar com amostras durante a fase de desenvolvimento, mas para analisar os resultados você deve utilizar os conjuntos de dados completos ou ao menos amostras maiores que considere relevantes.

Sempre que o produto de seus jobs for um conjunto de dados, este deve ser gerado na forma de partições que incluam ao menos data e hora (veja o formado dos conjuntos de dados para visitas e pedidos como exemplo), a menos que seja especificado algo diferente.

Fique atento aos jobs atuais e tenha em mente como você deve alterar e/ou criar novos.

O problema é montado de modo que um job gera dados para um ou mais jobs seguintes, fique atento ao reaproveitamento. Se quiser, você pode criar jobs para etapas intermediárias.

Muito cuidado com a organização de seu código: separar em módulos é uma boa prática, para permitir que os jobs sejam facilmente compreensíveis e atualizáveis. Cuide dos nomes de funções e variáveis, e fique atento aos seus resultados.

## Os dados

Os dados podem ser encontrados no repositório [b2wdigital/dataset-desafio-ia-front](https://github.com/b2wdigital/dataset-desafio-ia-front), na pasta, verá o seguinte conteúdo:

```BASH
dados
├── cep_coordenadas.csv
├── produtos.csv
├── visitas
├── pedidos
```

Aqui você tem alguns arquivos CSV e duas pastas. Os conjuntos de dados visitas e pedidos estão particionados pelas colunas `data` e `hora`. O schema de visitas é:

* product_id: Id do produto visitado, o mesmo id do dataset produtos.csv
* visit_id: Id da visita, este é um id único no dataset
* datetime: Datahora da visita no formato "yyyy-mm-dd HH:MM"
* prazo: Prazo de entrega exibido para o cliente
* preco: Preço do produto visitado ofertado pelo seller em questão
* frete: Valor do frete cobrado pela entrega do produto
* cep_prefixo: Inicio do cep para o qual o frete foi calculado
* coordenadas: Lista com latitude e longitude (nesta ordem) do cep de entrega
* data: data da visita
* hora: hora da visita

O dataset pedidos tem o seguinte schema:

* purchase_id: Id do pedido, este ítem deve ser único
* product_id: Id do produto visitado, o mesmo id do dataset produtos.csv
* visit_id: Id da visita que originou esse pedido, mesmo do dataset visitas
* data: data da visita que originou o pedido
* hora: hora da visita que originou o pedido

O dataset produtos.csv:

* product_id: Id do produto
* product_category_name: Departamento ao qual o produto pertence
* product_name_lenght: Número de caracteres no nome do produto
* product_description_lenght: Número de caracteres na descrição do produto
* product_photos_qty: Número de fotos do produto
* product_weight_g: Peso em gramas do produto
* product_length_cm: Profundidade do produto em centímetros
* product_height_cm: Altura do produto em centímetros
* product_width_cm: Largura do produto em centímetros

## Problema

### Associando visitas a pedidos

Primeiro passo é associar as visitas aos pedidos. O arquivo `desafio_iafront/jobs/pedidos.py` é o entrypoint dele. Este job coleta os dados de visitas, pedidos e produtos, e monta um dataset maior com todas as informações relevantes. Perceba que por questões de eficiência, esse job é particionado tanto por data quanto por hora, carregando os dados aos poucos para evitar sobrecarga de processamento.

Sua primeira missão é deixar o código mais limpo, para começar, vamos simplificar a função main. A partir de agora ela deve ser escrita no seguinte formato:

```Python
def main(pedidos, visitas, produtos, saida, data_inicial, data_final):
    produtos_df = read_csv(produtos)
    produtos_df["product_id"] = produtos_df["product_id"].astype(str)

    delta: timedelta = (data_final - data_inicial)
    date_partitions = [data_inicial.date() + timedelta(days=days) for days in range(delta.days)]

    for data in date_partitions:
        hour_partitions = list(range(0, 23))

        for hour in hour_partitions:
            hour_snnipet = f"hora={hour}"

            data_str = data.strftime('%Y-%m-%d')
            date_partition = f"data={data_str}"

            visitas_df = create_visitas_df(date_partition, hour_snnipet, visitas)

            pedidos_df = create_pedidos_df(date_partition, hour_snnipet, pedidos)

            visita_com_produto_e_conversao_df = merge_visita_produto(data_str, hour, pedidos_df, produtos_df,
                                                                     visitas_df)

            save_prepared(saida, visita_com_produto_e_conversao_df)
            print(f"Concluído para {date_partition} {hour}h")
```

Para tanto você deve implementar as seguintes funções e movê-las para um novo módulo dentro do módulo:

```Python
def save_prepared(saida: str, visita_com_produto_e_conversao_df: pd.DataFrame):
    pass


def merge_visita_produto(data_str: str, hour: int, pedidos_df: pd.DataFrame, produtos_df: pd.DataFrame, visitas_df: pd.DataFrame) -> pd.DataFrame:
    pass


def create_pedidos_df(date_partition: str, hour_snnipet: str, pedidos: pd.DataFrame) -> pd.DataFrame:
    pass


def create_visitas_df(date_partition: str, hour_snnipet: str, visitas: pd.DataFrame) -> pd.DataFrame:
    pass
```

Para simplificar ainda mais o código, a função main deve ser da seguinte forma:

```Python
@click.command()
@click.option('--pedidos', type=click.Path(exists=True))
@click.option('--visitas', type=click.Path(exists=True))
@click.option('--produtos', type=click.Path(exists=True))
@click.option('--saida', type=click.Path(exists=False, dir_okay=True, file_okay=False))
@click.option('--data-inicial', type=click.DateTime(formats=["%d/%m/%Y"]))
@click.option('--data-final', type=click.DateTime(formats=["%d/%m/%Y"]))
def main(pedidos, visitas, produtos, saida, data_inicial, data_final):
    produtos_df = read_csv(produtos)
    produtos_df["product_id"] = produtos_df["product_id"].astype(str)

    delta: timedelta = (data_final - data_inicial)
    date_partitions = [data_inicial.date() + timedelta(days=days) for days in range(delta.days)]

    for data in date_partitions:
        hour_partitions = list(range(0, 23))

        for hour in hour_partitions:
            date_partition = method_name(data, hour, pedidos, produtos_df, saida, visitas)
            print(f"Concluído para {date_partition} {hour}h")
```

Então você deve implementar uma nova função:

```Python
def method_name(data: str, hour: int, pedidos: str, produtos_df: pd.DataFrame, saida: str, visitas: str) -> pd.DataFrame:
    pass
```

### Scale de dados para analise semântica

Um dos problemas ao analisar dados no e-commerce está em relevar o que é importante para o usuário, uma das formas de fazer isso é por meio de semânticas. Uma das formas de construir relações semânticas é explorando a formação de clusters com o comportamento de alguma variável alvo, no caso, a conversão (o número de pedidos com relação ao número de visitas). Uma visita é dita convertida quando ela resulta numa compra, a razão entre número de compras pelo número de visitas de uma partição é chamada conversão. Vamos começar a próxima tarefa criando um job capaz de aplicar algumas outras técnicas de escala no dado o preparando para análise futura. O módulo `desafio_iafront/jobs/escala_pedidos/job_normalização.py` aplica uma técnica chamada normalização, sua próxima missão é se basear nele e criar novos jobs que apliquem as seguintes técnicas:

* StandardScaler¶
* MinMaxScaler
* MaxAbsScaler
* RobustScaler
* PowerTransformer

### Escalar dados

Sua próxima missão é criar dois novos jobs no módulo `desafio_iafront.jobs.graphics`

* Um deve ser capaz de criar um HTML com dois gráficos em formato scatter mostrando a distribuição dos pontos antes e depois da transformação de escala que você fez.
* Outro deve ser capaz de crirar gráficos no formato de histogramas mostrando a distribuição dos pontos antes e depois da transformação de escala que você fez.

Ambos os jobs devem ter como entrada (dentre mais) os dados que serão suados para cada eixo. Execute este gráfico para o Normalizer e as demais técnicas que implementou. Analisando os resultados para diversas colunas, você deve ser capaz de responder as seguintes perguntas:

* Usando uma semana de dados como entrada e vendo os gráficos, o que você pode dizer sobre cada uma das transformações?
* Use uma semana diferente, o que você viu mudando?
* Quais colunas escolheu para gerar suas análises, e por quê?
* Quais colunas sofreram maiores efeitos e quai sofretam menos?

### Gerar clusters

Agora que possui dados devidamente escalados, escolha os dados resultantes das três melhores transformações (na sua opinião) e aplique no job `desafio_iafront/jobs/clusters/job_kmeans.py`. Primeiro passo é alterar esse job para que o indicador do cluster resultante (`cluster_label`) seja um parâmetro de particionamento do conjunto de dados salvo. Isto é, o particionamento deve ser feito por `data`, `hora` e `cluster_label`, nessa, e outro para criar os reultados em ordem diferente: `cluster_label`, `data` e `hora`. Aqui o objetivo é otimizar o consumo dos dados posteriormente, permitindo leituras orientadas a cluster ou data.

Baseado nos jobs resultantes, agora você vai criar novos jobs para pelo menos quatro outros tipos de clusterização disponíveis no scikit learn. Estes podem ser encontrados no seguinte [link](https://scikit-learn.org/stable/modules/clustering.html#clustering), junto com a documentação de cada método. Você terá de explicar suas escolhas.

### Gráficos de clusters

Uma vez que você já tem os dados clusterizados, utilize a versão com o particionamento que lhe parecer mais interessante para criar novos jobs que geram gráficos para compar a distribuição dos pontos dos clusters. Antes de partir pra semântica, analise os gráficos, e com seu conhecimento matemático, explique quais combinações de método de escala e clusterização geram os melhores resultados.
### Agora você deve

Chegamos então ao ponto principal de nossa análise, agora vamos gerar e analisar a semântica de conversão. Você deverá criar alguns novos jobs:

* Um job que gera gráficos comparando a conversão total de cada cluster para uma janela de data de entrada
* Um novo job que calcula a conversão agregada em uma escala recebida como parâmetro que pode ser hora, minuto, ou dia. Ele deve salvar o resultado particionado por esta escala, e label do cluster.
* Um outro job que gera gráficos com séries temporais em uma escala recebida como parâmetro que pode ser hora, minuto, ou dia, comparando a evolução da conversão em cada cluster. Ele deve consumir como entrada o reultado do job anterior.

Agora analise todos os gráficos que você tem e responda, explicando o que lhe levou a estas conclusões:

* Quais clusters têm problemas para serem analisados?
* Quais clusters têm uma evolução ruim ao longo do tempo, e quais têm uma evolução boa.
* Quais clusters você considera que precisam de mais investimento para ampliar a conversão?
* Você consegue identificar algum fenômeno temporal que gere destaque a um ou mais clusters?

## Pipeline

* Como estágio final, você deve criar entradas na seção console_scripts do `setup.py` com comandos que executarão cada um dos seus jobs ao serem instalados.
* Seu projeto deve ser instalável em um virtualenv com o comando `pip3 install -e .` executado na raiz do projeto.
* Ao avaliar, devemos ser capazes de mudar os datasets (que terão os mesmos formatos mas dados diferentes, e os jobs devem continuar funcionando)
* Crie um script shell ou um Makefile (será um bônus se fizer um Makefile) com o pipeline de execução.
