# Estudos SDP - Spark Declarative Pipelines 🚀

Este é um projeto educacional dedicado ao estudo e implementação de **Spark Declarative Pipelines (SDP)** utilizando o **Apache Spark 4.1.3**. 

O objetivo principal é construir do zero (manualmente) a infraestrutura de um cluster Spark e implementar pipelines de dados utilizando o paradigma declarativo, onde as transformações são definidas como *Materialized Views* em vez de scripts imperativos complexos.

## 🎯 Objetivos do Projeto

1. **Construção Manual de Infraestrutura**: Evitar imagens pré-configuradas para entender a instalação do Java 17, binários do Spark 4.x e configuração de variáveis de ambiente.
2. **Orquestração com Docker**: Implementar uma topologia de Master e Workers com limites de recursos para estabilidade do host.
3. **Domínio do SDP**: Utilizar a biblioteca `pyspark[pipelines]` para criar fluxos de dados via decorators `@dp`, gerenciando dependências de forma automática.
4. **Persistência Moderna**: Integrar o catálogo **Apache Iceberg** para armazenamento de tabelas com suporte a ACID e evolução de schema.

## 🛠️ Stack Tecnológica

- **Apache Spark 4.1.3**: Motor de processamento distribuído.
- **Java 17 (OpenJDK)**: Base necessária para a JVM do Spark 4.x.
- **PySpark [pipelines]**: Extensão para a implementação de pipelines declarativos.
- **Apache Iceberg**: Formato de tabela para o Data Lakehouse.
- **Docker & Docker Compose**: Isolamento e orquestração do cluster.

## 🏗️ Arquitetura do Projeto

O repositório está dividido em duas partes principais:

### 1. `spark-cluster/` (Infraestrutura)
Contém tudo o que é necessário para subir o ambiente:
- **Dockerfile**: Instalação manual do Java, Spark e jars do Iceberg.
- **Docker Compose**: Define o `spark-master` e `spark-worker`.
- **Scripts de Entrypoint**: Controlam a inicialização dos processos do Spark e a coleta de logs.

### 2. `sdp-project/` (Lógica de Negócio)
Onde reside a implementação do pipeline:
- **`spark-pipeline.yml`**: Especificação do projeto (catálogo, banco de dados e bibliotecas).
- **`transformations/`**: Módulos Python contendo as funções decoradas com `@dp.materialized_view`.
- **`data/`**: Arquivos CSV de entrada para processamento.

## 🚀 Como Executar

O projeto possui um script de automação que realiza todo o ciclo: build do cluster $\rightarrow$ sincronização de dados $\rightarrow$ execução do pipeline $\rightarrow$ query de resultados.

```bash
# Dê permissão de execução ao script
chmod +x run-sdp.sh

# Execute o pipeline completo
./run-sdp.sh
```

### Detalhes da Execução:
1. **Cluster**: Sobe o Master e Worker via Docker Compose.
2. **Sincronização**: Copia os arquivos CSV e a pasta do projeto para dentro do container `spark-master`.
3. **Processamento**: Executa o comando `spark-pipelines run` dentro do container.
4. **Verificação**: Realiza um `SELECT` automático no catálogo Iceberg para exibir os resultados processados.

## 🗺️ Roadmap de Estudos

- [x] Dockerfile manual com Java 17 e Spark 4.1.3.
- [x] Docker Compose com topologia Master-Worker.
- [x] Instalação de `pyspark[pipelines]`.
- [x] Primeira pipeline SDP com Materialized Views.
- [x] Automação via `run-sdp.sh`.
- [ ] Expansão da lógica de transformação e dependências complexas.

---
*Projeto desenvolvido para fins de estudo sobre a evolução do processamento de dados com Apache Spark.*
