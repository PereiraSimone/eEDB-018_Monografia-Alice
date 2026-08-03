📚 TCC: Framework A.L.I.C.E. (Documentação Oficial)

Monografia de Especialização em Engenharia de Dados e Big Data

Instituição: Escola Politécnica da Universidade de São Paulo (PECE-Poli)

Autora: Simone Pereira Pinto

📌 Sobre esta Branch (main_documentation)

Esta é branch central de Documentação deste repositório! O objetivo desta branch é versionar exclusivamente a parte escrita do TCC, mantendo o histórico de revisões dos arquivos de texto (Word/PDF), diagramas arquiteturais e rascunhos metodológicos separados do código-fonte da aplicação.

🚀 Sobre o Projeto: Framework A.L.I.C.E.

A.L.I.C.E. (Adaptive Logic for Ingestion and Continuous Evaluation) é um framework agnóstico e orquestrador de qualidade.

O projeto nasce para resolver a lacuna entre a Engenharia de Software Clássica e o ecossistema de Big Data. Baseado no paradigma de Configuration-as-Code (CaC), o framework integra o motor declarativo do Soda Core ao processamento distribuído do PySpark.

Principais Atuações:

🛡️ Quality Gate (CI/CD): Valida a estrutura dos contratos de dados no momento do deploy.

🛑 Circuit Breaker (Airflow): Intercepta anomalias estruturais no fluxo contínuo de dados.

📊 Observabilidade: Gera métricas e alertas de saúde dos dados para consumo das equipes de negócio e engenharia.

📂 Estrutura de Diretórios

📦 main_documentation/ ┣ 📂 01_rascunhos/ # Rascunhos, anotações soltas e feedbacks do orientador ┣ 📂 02_diagramas/ # Imagens e códigos-fonte dos diagramas (ex: C4 Model em Mermaid) ┣ 📂 03_referencias/ # PDFs de artigos, livros e relatórios (ex: Gartner, ISTQB) ┣ 📜 Monografia_Simone_vX.docx # Versão atual de trabalho do documento oficial ┗ 📜 README.md # Este arquivo

🛠️ Stack Tecnológico Abordado

Linguagem: Python

Processamento Distribuído: Apache Spark (PySpark)

Motor de Qualidade: Soda Core

Arquitetura Visual: C4 Model (via Mermaid.js)

Orquestração e Deploy: Ferramentas de CI/CD e Apache Airflow

📝 Status Atual do Documento

[x] Capa, Folha de Rosto e Resumo (Em andamento)

[x] Introdução e Motivação

[x] Objetivos (Geral e Específicos) (Em andamento)

[ ] Metodologia (Desenho Arquitetural) (Em andamento)

[ ] Metodologia (Prova de Conceito - PoC) (Em andamento)

[ ] Fundamentação Teórica

[ ] Resultados e Conclusão

🗺️ Mapa Next Steps: TCC A.L.I.C.E.

✅ Passo 1: 

[x] Tema validado.

[x] Resumo, Introdução e Motivação

[x] Objetivo Geral e arquitetura (Soda Core + PySpark).

🚧 Passo 2: Lapidação do Capítulo 1 e 3 

[x] Aplicar  correções gramaticais e de numeração ABNT 

[ ]  Desenho da Prova de Conceito (PoC). Em andamento

[ ] Definir qual base de dados usar para testar o ALICE 
    (IDEIA: pegar um dataset público do Kaggle, injetar erros nele propositalmente e mostrar o ALICE bloqueando).

⏳ Passo 3: (Capítulo 2)

[ ] Construir o Capítulo 2 - Fundamentação Teórica.

[ ] Escrever sobre a evolução do Big Data (Os 5V's).

[ ] Escrever sobre o choque entre Engenharia de Software Tradicional (ISTQB) e DataOps.

[ ] Explicar o paradigma Configuration-as-Code.

🚀 Passo 4: Capítulo 4 - Desenvolvimento

[ ] Codificar a PoC - script Python/PySpark integrando com o arquivo YAML do Soda Core

[ ] Escrever o Capítulo 4 - Desenvolvimento, colocar os prints do código, as capturas de tela do CI/CD rodando e os alertas sendo gerados.

🏁 Passo 5: Fechamento (Capítulo 5)

[ ] Escrever a Conclusão, confirmando que o framework cumpriu o seu papel de circuit breaker.

[ ] Formatação final (Sumário, Referências Bibliográficas).