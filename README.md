# PSPD-Spark-Kafka-Twitter

Este repositório busca implementar casos de estudo para manipulação e processamento de grande volumes de dados (Big Data). A execução desse projeto foi em um cluster com dois nós.

## 1. Configurar o ambiente e o cluster para execução

Para configuração do ambiente para a criação de um cluster com máquinas virtuais o tutorial utilizado foi:
 https://www.linkedin.com/pulse/how-setup-install-apache-spark-311-cluster-ubuntu-shrivastava/?trk=pulse-article_more-articles_related-content-card 

## 2. Configurar o Jupyter Notebook

Executar este comando em todos as máquinas do cluster( Master + Workers ):
```
  sudo apt-get install python3 curl
```
```
  curl -sS https://bootstrap.pypa.io/get-pip.py | sudo python3
```
```
  sudo pip3 install jupyter py4j
```

algumas variáveis de ambiente configuradas:
```
  export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
  export PYSPARK_DRIVER_PYTHON="jupyter"
  export PYSPARK_DRIVER_PYTHON_OPTS="notebook"
  export PYSPARK_PYTHON=python3
```

para utilização do jupyter devem ser feitas algumas configurações de permissão:
* sudo chmod 777 <SPARK_HOME>
* sudo chmod 777 <SPARK_HOME>/python
* sudo chmod 777 pyspark (executar dentro da pasta <SPARK_HOME>/python)


Obs: em caso de dúvidas foi seguido o seguinte tutorial para configuração <https://www.youtube.com/watch?v=9bSYHKj1LUY&list=PL0hSJrxggIQrx4kePKiTsH1MpXVs9GC_V&index=2>

## 3. Instalação e configuração Kafka

Foi seguido o tutorial oficial da apache para configuração, nesse tutorial também ensina como iniciar os servidores e o broker. https://kafka.apache.org/quickstart

## 4. Criar o tópico para o streaming de dados

O tópico a ser criado terá como nome "twitter-data" que pode ser iniciado com o comando: 

```
  cd <local_de_instalacao_kafka>
```

```
bin/kafka-topics.sh --create --topic twitter-data --bootstrap-server localhost:9092
```


## 5. Configurar tweepy

dentro da pasta parte1 tem um arquivo .env.template em que deve ser colocado os seus tokens de acesso e todas as informações oferecidas pelo tweepy. [Cadastro Developer para API do Twitter](https://developer.twitter.com/en)

## 6. Iniciar a aplicação

Na pasta do projeto, como o jupyter instalado é necessário utilizar o comando para iniciar o jupyter:
```
  jupyter notebook
```

Com isso ficam abertos para todas as aplicações quem a execução deve ser seguida conforme a ordem dos notebooks.

Para gerar os dados para consumo nas aplicações deve primeiro ser iniciado o arquivo kafka_twitter_producer.


### As aplicações implementadas são:
* **Numero de ocorrências de uma palavra que começa com S, P ou R em um determinado período de tempo** - parte1/number_of_ocurrences_spr
* **Número de palavras contabilizadas com tamanho igual a 6, 8 ou 11 em um período de tempo** - parte1/number_of_words_by_len
* **Ocorrência de palavras com nuvem de palavras** - parte1/wordcloud
* **Número total de palavras contabilizadas em um período de tempo** - parte1/total_wordcount

* **Análise de sentimento de forma a estipular o índice de aprovação e reprovação de um candidato a presidência** - parte2/political_analysis



