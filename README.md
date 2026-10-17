# 🛡️ Insurance Lakehouse Pipeline | Databricks & Unity Catalog

[![Databricks](https://img.shields.io/badge/Databricks-Free%20Edition-red?style=flat-square&logo=databricks)](https://databricks.com/)
[![Delta Lake](https://img.shields.io/badge/Storage-Delta%20Lake-blue?style=flat-square)](https://delta.io/)
[![Python](https://img.shields.io/badge/Language-PySpark-yellow?style=flat-square&logo=python)](https://spark.apache.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg?style=flat-square)](https://opensource.org/licenses/MIT)

> **Projeto de Portfólio & Demonstração Técnica**  
> **Autor:** Edu Bello | Data Engineering & Analytics  
> **Contato & Perfil:** [LinkedIn](https://linkedin.com/in/) | [GitHub](https://github.com/edubello)

---

## 📌 Visão Geral do Projeto

Este repositório contém a implementação completa de uma **Arquitetura Lakehouse End-to-End** no setor de **Seguros (Insurance)**, construída no **Databricks Free Edition** e governada pelo **Unity Catalog**.

O objetivo deste projeto é demonstrar as melhores práticas de Engenharia de Dados modernas, abrangendo desde a simulação e ingestão de dados brutos até a entrega de camadas analíticas prontas para BI e consumo de negócios.

---

## 🏗️ Arquitetura & Camadas de Dados (Medallion)

A solução segue o padrão de arquitetura Medallion (`raw` ➔ `bronze` ➔ `silver` ➔ `gold`), isolada no catálogo `insurance` dentro do Unity Catalog:

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                       UNITY CATALOG: `insurance`                            │
└─────────────────────────────────────────────────────────────────────────────┘
       │
       ├── 📂 raw.landing (Volume)      ➔ Ingestão de arquivos brutos (CSV/JSON)
       ├── 📂 bronze (Schema)           ➔ Dados em Delta Lake + Metadados
       ├── 📂 silver (Schema)           ➔ Dados limpos, validados e enriquecidos
       └── 📂 gold (Schema)             ➔ Modelo dimensional (Fatos & Dimensões)
