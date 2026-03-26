from neo4j import GraphDatabase
from groq import Groq

# -----------------------------
# Neo4j Connection
# -----------------------------
import streamlit as st
from neo4j import GraphDatabase

driver = GraphDatabase.driver(
    st.secrets["NEO4J_URI"],
    auth=(
        st.secrets["NEO4J_USER"],
        st.secrets["NEO4J_PASSWORD"]
    )
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

    # Deliveries
    if any(word in user_input for word in ["delivery", "deliveries"]):
        return """
MATCH (d:Delivery)
RETURN d.deliveryId AS deliveryId
LIMIT 10
"""

    # Products
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


def get_graph_data(
    customerIds=None,
    deliveryIds=None,
    productIds=None,
    salesOrderIds=None,
    billingDocumentIds=None,
):
    """
    Fetch a small slice of the Neo4j graph for visualization.

    Returns a structured "Order to Cash" business subgraph:
      Customer -> SalesOrder -> Delivery -> Product
    (via DeliveryItem -> Product)

    Optional filters can be applied by passing lists of IDs:
      - customerIds: Customer.customerId
      - deliveryIds: Delivery.deliveryId
      - productIds: Product.productId
      - salesOrderIds: SalesOrder.salesOrderId
      - billingDocumentIds: BillingDocument.billingDocumentId (resolves linked Delivery via HAS_BILLING)

    Returns:
      {
        "nodes": [{"id": <elementId>, "labels": [...], "properties": {...}}, ...],
        "edges": [{"from": <elementId>, "to": <elementId>, "type": <relType>, "properties": {...}}, ...]
      }
    """
    def _clean_list(v):
        if v is None:
            return None
        if isinstance(v, (list, tuple, set)):
            values = [str(x) for x in v if x is not None and str(x).strip() != ""]
        else:
            values = [str(v)]
        return values or None

    customerIds = _clean_list(customerIds)
    deliveryIds = _clean_list(deliveryIds)
    productIds = _clean_list(productIds)
    salesOrderIds = _clean_list(salesOrderIds)
    billingDocumentIds = _clean_list(billingDocumentIds)

    focused = any(
        x is not None
        for x in (customerIds, deliveryIds, productIds, salesOrderIds, billingDocumentIds)
    )
    # Focused: enough rows for O2C chains; explore: tiny sample so the graph never looks "full dataset".
    row_limit = 130 if focused else 18
    max_nodes_cap = 150 if focused else 55
    max_edges_cap = 280 if focused else 160

    # Structured business flow graph:
    # Customer -> SalesOrder -> Delivery -> Product
    # Product is derived from delivery items -> REFERENCES_PRODUCT.
    cypher = """
    MATCH (c:Customer)-[:PLACES]->(so:SalesOrder)
    OPTIONAL MATCH (so)-[:HAS_DELIVERY]->(d:Delivery)

    // Attempt to follow a direct "CONTAINS_PRODUCT" if it exists;
    // otherwise fall back to the delivery-item reference product path.
    OPTIONAL MATCH (so)-[:CONTAINS_PRODUCT]->(p_from_so:Product)
    OPTIONAL MATCH (d)-[:HAS_ITEM]->(:DeliveryItem)-[:REFERENCES_PRODUCT]->(p_from_delivery:Product)
    WITH
      c, so, d,
      coalesce(p_from_so, p_from_delivery) AS p
    WHERE
      ($customerIds IS NULL OR c.customerId IN $customerIds)
      AND ($salesOrderIds IS NULL OR so.salesOrderId IN $salesOrderIds)
      AND ($deliveryIds IS NULL OR (d IS NOT NULL AND d.deliveryId IN $deliveryIds))
      AND ($productIds IS NULL OR (p IS NOT NULL AND p.productId IN $productIds))
    RETURN DISTINCT c, so, d, p
    LIMIT $rowLimit
    """

    nodes_by_id = {}
    edges = []
    edge_keys = set()

    with driver.session() as session:
        # Resolve deliveries linked to billing docs (Delivery)-[:HAS_BILLING]->(BillingDocument)
        delivery_id_set = set(deliveryIds or [])
        if billingDocumentIds:
            for rec in session.run(
                """
                MATCH (d:Delivery)-[:HAS_BILLING]->(bd:BillingDocument)
                WHERE bd.billingDocumentId IN $billingDocumentIds
                RETURN DISTINCT d.deliveryId AS deliveryId
                """,
                billingDocumentIds=billingDocumentIds,
            ):
                did = rec.get("deliveryId")
                if did is not None:
                    delivery_id_set.add(str(did))
        deliveryIds = sorted(delivery_id_set) if delivery_id_set else None

        # Billing-only focus with no resolved deliveries and no other filters → empty subgraph.
        if billingDocumentIds and not delivery_id_set:
            if not any([customerIds, productIds, salesOrderIds]):
                return {"nodes": [], "edges": []}

        for record in session.run(
            cypher,
            customerIds=customerIds,
            deliveryIds=deliveryIds,
            productIds=productIds,
            salesOrderIds=salesOrderIds,
            rowLimit=row_limit,
        ):
            c = record.get("c")
            so = record.get("so")
            d = record.get("d")
            p = record.get("p")

            if c is not None:
                c_id = c.element_id
                if c_id not in nodes_by_id:
                    nodes_by_id[c_id] = {
                        "id": c_id,
                        "labels": list(c.labels),
                        "properties": dict(c),
                    }
            else:
                continue

            if so is not None:
                so_id = so.element_id
                if so_id not in nodes_by_id:
                    nodes_by_id[so_id] = {
                        "id": so_id,
                        "labels": list(so.labels),
                        "properties": dict(so),
                    }

                # Customer -> SalesOrder
                edge_key = (c.element_id, so_id, "PLACES")
                if edge_key not in edge_keys:
                    edge_keys.add(edge_key)
                    edges.append({
                        "from": c.element_id,
                        "to": so_id,
                        "type": "PLACES",
                        "properties": {},
                    })

            # SalesOrder -> Delivery
            if d is not None and so is not None:
                d_id = d.element_id
                if d_id not in nodes_by_id:
                    nodes_by_id[d_id] = {
                        "id": d_id,
                        "labels": list(d.labels),
                        "properties": dict(d),
                    }
                edge_key = (so_id, d_id, "HAS_DELIVERY")
                if edge_key not in edge_keys:
                    edge_keys.add(edge_key)
                    edges.append({
                        "from": so_id,
                        "to": d_id,
                        "type": "HAS_DELIVERY",
                        "properties": {},
                    })

            # Delivery -> Product (derived)
            if p is not None and d is not None:
                p_id = p.element_id
                if p_id not in nodes_by_id:
                    nodes_by_id[p_id] = {
                        "id": p_id,
                        "labels": list(p.labels),
                        "properties": dict(p),
                    }
                edge_key = (d.element_id, p_id, "HAS_PRODUCT")
                if edge_key not in edge_keys:
                    edge_keys.add(edge_key)
                    edges.append({
                        "from": d.element_id,
                        "to": p_id,
                        "type": "HAS_PRODUCT",
                        "properties": {},
                    })

    nodes = list(nodes_by_id.values())

    if len(nodes) > max_nodes_cap:
        keep_ids = set(n["id"] for n in nodes[:max_nodes_cap])
        nodes = [n for n in nodes if n["id"] in keep_ids]
        edges = [e for e in edges if e.get("from") in keep_ids and e.get("to") in keep_ids]

    if len(edges) > max_edges_cap:
        edges = edges[:max_edges_cap]

    return {"nodes": nodes, "edges": edges}