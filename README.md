# SAP Order-to-Cash Graph Intelligence App

An AI-powered graph analytics application for exploring SAP Order-to-Cash (O2C) data using Neo4j and natural language queries.

---

## Overview

This project enables users to explore relationships between Customers, Orders, Deliveries, and Products using a graph-based approach combined with natural language querying.

The system converts user questions into Cypher queries using an LLM, executes them on a Neo4j graph database, and visualizes the results interactively.

---

## Architecture

User Query (Natural Language)  
-> LLM (Groq API) converts query to Cypher  
-> Backend executes Cypher on Neo4j  
-> Results returned and visualized  
-> Streamlit UI renders graph and chat interface  

## Architecture Diagram

```mermaid
flowchart TD

A[User Query (Natural Language)] --> B[Streamlit UI]

B --> C[Groq LLM]
C -->|Generate Cypher Query| D[Backend]

D --> E[Neo4j AuraDB]

E -->|Query Results| D
D --> F[Graph Visualization (PyVis)]

F --> B
```

### Key Design Decisions

- Used a **graph-based architecture** because SAP O2C data is highly relational (customer → order → delivery → product).
- Decoupled components:
  - UI (Streamlit)
  - Backend (query execution + LLM integration)
  - Database (Neo4j)
- LLM is used only for **query generation**, not for storing or retrieving data directly.

---

## Database Choice

### Why Neo4j?

- SAP O2C data naturally forms a graph:
  - Customers place Orders
  - Orders generate Deliveries
  - Deliveries include Products
- Neo4j allows:
  - Efficient relationship traversal
  - Flexible schema for evolving enterprise data
  - Powerful querying using Cypher

### Cloud Deployment

- Initially developed using local Neo4j
- Migrated to **Neo4j AuraDB (cloud)** for deployment compatibility
- Enabled external access from Streamlit Cloud

---

## LLM Prompting Strategy

The LLM is used to convert natural language into Cypher queries.

### Approach

- Structured prompts instruct the model to:
  - Only generate Cypher queries
  - Use known node labels (Customer, Order, Delivery, Product)
  - Follow valid relationship patterns

### Example Prompt Pattern

"Convert the following question into a Cypher query using the SAP O2C schema. Only return the query."

### Design Choices

- Avoid free-form responses from LLM
- Enforce query-only outputs
- Keep prompts simple and deterministic to reduce hallucination

---

## Guardrails

To ensure safety and correctness:

### 1. Query Restriction
- Only SAP dataset-related queries are allowed
- Prevents unrelated or unsafe queries

### 2. Controlled LLM Output
- LLM is instructed to return only Cypher queries
- No execution of arbitrary code

### 3. Secure Secret Handling
- API keys stored in `.env` (local)
- Stored in Streamlit secrets for deployment
- Never committed to GitHub

### 4. Deployment Safety
- Removed all hardcoded credentials
- Prevented GitHub push protection violations

---

## Tech Stack

- Frontend: Streamlit  
- Backend: Python  
- Database: Neo4j AuraDB  
- LLM: Groq API  
- Visualization: PyVis  

---

## Features

- Natural language querying of graph data
- Real-time Cypher execution
- Interactive graph visualization
- Cloud deployment support

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

## Key Highlights

- End-to-end AI + Graph system
- Cloud deployment with Neo4j AuraDB
- Real-time natural language querying
- Clean separation of components

---

## Future Improvements

- Path highlighting between nodes
- Advanced filtering options
- Multi-hop reasoning with LLM
- Improved UI responsiveness

---

## Author

Surya Srinivasan  
B.Tech Computer Science Engineering  
VIT Vellore
