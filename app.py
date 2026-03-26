from neo4j import GraphDatabase
from groq import Groq

# -----------------------------
# Neo4j Connection
# -----------------------------
driver = GraphDatabase.driver(
    "bolt://localhost:7687",
    auth=("neo4j", "Surya1306@")
)

# -----------------------------
# Groq LLM Setup
# -----------------------------
import os
from dotenv import load_dotenv

load_dotenv()

client = Groq(api_key=os.getenv("GROQ_API_KEY"))


# -----------------------------
# Run Cypher Query
# -----------------------------
def run_query(query):
    with driver.session() as session:
        return [record.data() for record in session.run(query)]


# -----------------------------
# Guardrail (Domain Check)
# -----------------------------
def is_valid_question(user_input: str) -> bool:
    user_input = user_input.lower()

    keywords = [
        "customer", "customers",
        "order", "orders", "sales",
        "delivery", "deliveries",
        "billing", "invoice",
        "payment", "payments",
        "product", "products",
        "plant"
    ]

    return any(word in user_input for word in keywords)


# -----------------------------
# Clean LLM Output
# -----------------------------
def clean_query(query: str) -> str:
    query = query.strip()

    if "```" in query:
        parts = query.split("```")
        if len(parts) > 1:
            query = parts[1]
        query = query.replace("cypher", "").replace("sql", "").strip()

    if query.lower().startswith("cypher"):
        query = query[6:].strip()

    if query.lower().startswith("sql"):
        query = query[3:].strip()

    return query


# -----------------------------
# Fix LLM Mistakes + Guardrails
# -----------------------------
def fix_query(query: str, user_input: str) -> str:
    user_input = user_input.lower()

    # 🎯 Top customers
    if "customer" in user_input and ("highest" in user_input or "top" in user_input):
        return """
MATCH (c:Customer)-[:PLACES]->(so:SalesOrder)
RETURN c.customerId AS customerId, count(so) AS totalOrders
ORDER BY totalOrders DESC
LIMIT 10
"""

    # 🎯 Deliveries
    if any(word in user_input for word in ["delivery", "deliveries"]):
        return """
MATCH (d:Delivery)
RETURN d.deliveryId AS deliveryId
LIMIT 10
"""

    # 🎯 Products
    if any(word in user_input for word in ["product", "products"]):
        return """
MATCH (p:Product)
RETURN p.productId AS productId
LIMIT 10
"""

    # generic fixes
    query = query.replace("sov", "so")
    query = query.replace("c.name", "c.customerId")

    # prevent nested aggregation
    if "collect(" in query and "count(" in query:
        return """
MATCH (c:Customer)-[:PLACES]->(so:SalesOrder)
RETURN c.customerId AS customerId, count(so) AS totalOrders
ORDER BY totalOrders DESC
LIMIT 10
"""

    return query


# -----------------------------
# LLM → Cypher
# -----------------------------
def llm_to_query(user_input):
    print("🔥 USING LLM")

    prompt = f"""
Convert the following question into a Neo4j Cypher query.

Schema:
(:Customer)-[:PLACES]->(:SalesOrder)
(:SalesOrder)-[:HAS_ITEM]->(:SalesOrderItem)
(:SalesOrder)-[:HAS_DELIVERY]->(:Delivery)
(:Delivery)-[:HAS_BILLING]->(:BillingDocument)
(:BillingDocument)-[:HAS_PAYMENT]->(:Payment)
(:SalesOrderItem)-[:REFERENCES_PRODUCT]->(:Product)

Rules:
- Only return Cypher query
- No explanation
- Avoid invalid properties
- Avoid nested aggregations
- Add LIMIT 10 if not aggregation

Question:
{user_input}
"""

    response = client.chat.completions.create(
        model="llama-3.1-8b-instant",
        messages=[{"role": "user", "content": prompt}]
    )

    query = response.choices[0].message.content.strip()

    query = clean_query(query)
    query = fix_query(query, user_input)

    return query


# -----------------------------
# Format Response (FINAL FIXED)
# -----------------------------
def format_response(result, user_input):
    if not result:
        return "No results found."

    user_input = user_input.lower()

    # Customers
    if "customer" in user_input and "order" in user_input:
        return "\n".join([
            f"Customer {r['customerId']} has {r['totalOrders']} orders"
            for r in result
        ])

    # Deliveries (FIXED)
    if "delivery" in user_input or "deliveries" in user_input:
        return "\n".join([
            f"Delivery ID: {r.get('deliveryId', 'Unknown')}"
            for r in result
        ])

    # Products
    if "product" in user_input:
        return "\n".join([
            f"Product ID: {r.get('productId', 'Unknown')}"
            for r in result
        ])

    # fallback
    return str(result)


# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    print("💬 Ask something about your SAP data (type 'exit' to quit)\n")

    while True:
        user_input = input("👉 You: ")

        if user_input.lower() == "exit":
            print("👋 Exiting...")
            break

        # Guardrail
        if not is_valid_question(user_input):
            print("🚫 This system only answers SAP dataset-related queries.")
            continue

        try:
            query = llm_to_query(user_input)
            print("\n🧠 Generated Query:\n", query)

            result = run_query(query)

            response = format_response(result, user_input)

            print("\n💬 Answer:")
            print(response)

        except Exception as e:
            print("❌ ERROR:", e)