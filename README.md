# SAP Order-to-Cash Graph Intelligence App

An AI-powered graph analytics application for exploring SAP Order-to-Cash (O2C) data using Neo4j and natural language queries.

---

## Overview

This project enables users to explore complex relationships between Customers, Orders, Deliveries, and Products using:

- Graph visualization
- Natural language querying (LLM-powered)
- Real-time graph database interaction

Example queries:
- Top customers by number of orders
- Show order to delivery flow
- Most delivered products

---

## Architecture

User Query (Natural Language)
-> Groq LLM (Query to Cypher)
-> Neo4j AuraDB (Graph Database)
-> Backend (Python)
-> Streamlit UI + PyVis Visualization

---

## Tech Stack

- Frontend: Streamlit
- Backend: Python
- Database: Neo4j AuraDB (Cloud)
- LLM: Groq API
- Visualization: PyVis

---

## Features

- Graph-based exploration of SAP O2C data
- Natural language to Cypher query conversion
- Real-time querying from Neo4j
- Cloud deployment using Streamlit
- Interactive graph visualization

---

## Live Demo

https://sap-o2c-graph-janxfv3sxqukse7mvzngob.streamlit.app

---

## Project Structure

sap-o2c-graph/
│
├── app.py
├── backend.py
├── ui.py
├── load_sap_o2c_to_neo4j.py
├── requirements.txt
├── .gitignore
└── README.md

---

## Setup (Local)

### 1. Clone repository

git clone https://github.com/SURYAS1306/sap-o2c-graph.git
cd sap-o2c-graph

### 2. Install dependencies

pip install -r requirements.txt

### 3. Configure environment variables

Create a `.env` file:

GROQ_API_KEY=your_key  
NEO4J_URI=your_uri  
NEO4J_USER=neo4j  
NEO4J_PASSWORD=your_password  

### 4. Run the application

streamlit run ui.py

---

## Environment Variables

- GROQ_API_KEY: Groq LLM API key  
- NEO4J_URI: AuraDB connection URI  
- NEO4J_USER: Neo4j username  
- NEO4J_PASSWORD: Neo4j password  

---

## LLM Prompting Strategy

- Convert natural language queries into Cypher queries
- Restrict responses to SAP dataset scope
- Ensure structured output for execution in Neo4j
- Minimize hallucinations through controlled prompting

---

## Guardrails

- Only SAP dataset-related queries are allowed
- Controlled Cypher generation to avoid unsafe queries
- API keys are not stored in code
- Secrets handled via environment variables and Streamlit secrets

---

## Key Highlights

- End-to-end AI and graph-based system
- Fully deployed cloud application
- Real-time interaction with Neo4j database
- Scalable architecture for enterprise use cases

---

## Future Improvements

- Path highlighting between entities
- Advanced graph filtering
- Multi-hop reasoning using LLM
- Improved UI interactions

---

## Author

Surya Srinivasan  
B.Tech Computer Science Engineering  
VIT Vellore

---

## Notes

This project demonstrates how AI and graph databases can be combined to simplify exploration of enterprise data through natural language interfaces.
