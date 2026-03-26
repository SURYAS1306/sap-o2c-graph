import tempfile
from typing import Any, Dict

import streamlit as st
import streamlit.components.v1 as components
from pyvis.network import Network

from backend import (
    llm_to_query,
    run_query,
    format_response,
    is_valid_question,
    get_graph_data,
)


st.set_page_config(
    page_title="Order to Cash Graph",
    layout="wide",
)


st.markdown(
    """
<style>
body, .stApp {
    background: #f6f7fb;
    color: #111827;
    font-family: 'Inter', sans-serif;
}

/* Fix chat visibility without breaking layout */
[data-testid="stChatMessage"] {
    background-color: #f9fafb !important;  /* light grey instead of pure white */
    color: #111827 !important;            /* force dark text */
}

[data-testid="stChatMessage"] p {
    color: #111827 !important;
}
.card {
    background: white;
    border-radius: 16px;
    padding: 20px;
    box-shadow: 0 2px 12px rgba(0,0,0,0.08);
    margin-bottom: 20px;
}

.card h3, .card h2, .card h1, .card .stSubheader {
    margin-top: 0px;
}
</style>
""",
    unsafe_allow_html=True,
)


def _primary_label(labels: Any) -> str:
    if not labels:
        return "Node"
    if isinstance(labels, list) and labels:
        return str(labels[0])
    return str(labels)


def _escape_html(s: str) -> str:
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#x27;")
    )


def render_graph(graph_data: Dict[str, Any]) -> None:
    """
    Render an interactive pyvis graph from `get_graph_data()` payload.
    """
    nodes = graph_data.get("nodes", []) if graph_data else []
    edges = graph_data.get("edges", []) if graph_data else []

    highlight = st.session_state.get("graph_highlight", {}) or {}
    highlight_customer_ids = set(map(str, highlight.get("customerIds", []) or []))
    highlight_delivery_ids = set(map(str, highlight.get("deliveryIds", []) or []))
    highlight_product_ids = set(map(str, highlight.get("productIds", []) or []))
    highlight_sales_order_ids = set(map(str, highlight.get("salesOrderIds", []) or []))

    node_is_highlighted = {}

    net = Network(
        height="650px",
        width="100%",
        bgcolor="#fafbfc",
        font_color="#374151",
        directed=True,
    )
    net.toggle_physics(True)
    # Minimal, airy network: specified forceAtlas2 + soft flowing edges.
    net.set_options("""
    var options = {
      "physics": {
        "enabled": true,
        "solver": "forceAtlas2Based",
        "forceAtlas2Based": {
          "gravitationalConstant": -20,
          "centralGravity": 0.005,
          "springLength": 180,
          "springConstant": 0.02,
          "damping": 0.95
        },
        "minVelocity": 0.75
      },
      "edges": {
        "smooth": { "type": "continuous" },
        "color": { "inherit": false, "color": "rgba(150, 200, 255, 0.3)" },
        "selectionWidth": 0.75
      },
      "layout": { "improvedLayout": true },
      "interaction": { "hover": true, "tooltipDelay": 100 }
    }
    """)

    NODE_DEFAULT = "#6baed6"
    NODE_HIGHLIGHT = "#ff6b6b"
    EDGE_RGBA = "rgba(150, 200, 255, 0.3)"

    for n in nodes:
        node_id = str(n.get("id"))
        labels = n.get("labels", [])
        label = _primary_label(labels)
        props = n.get("properties", {}) or {}

        # Highlight relevant nodes (from query result) and display meaningful IDs.
        customer_id = props.get("customerId")
        delivery_id = props.get("deliveryId")
        product_id = props.get("productId")

        # Highlight logic:
        # - If we have explicit IDs for a node type, highlight only matching nodes of that type.
        # - If we don't have explicit IDs for that node type, keep the whole flow readable (not faded).
        is_highlighted = True
        if label == "Customer":
            is_highlighted = (not highlight_customer_ids) or (
                customer_id is not None and str(customer_id) in highlight_customer_ids
            )
        elif label == "SalesOrder":
            so_id = props.get("salesOrderId")
            is_highlighted = (not highlight_sales_order_ids) or (
                so_id is not None and str(so_id) in highlight_sales_order_ids
            )
        elif label == "Delivery":
            is_highlighted = (not highlight_delivery_ids) or (
                delivery_id is not None and str(delivery_id) in highlight_delivery_ids
            )
        elif label == "Product":
            is_highlighted = (not highlight_product_ids) or (
                product_id is not None and str(product_id) in highlight_product_ids
            )

        node_is_highlighted[node_id] = is_highlighted

        display_label = label
        if label == "Customer" and customer_id is not None:
            display_label = f"Customer {customer_id}"
        elif label == "SalesOrder":
            so_id = props.get("salesOrderId")
            if so_id is not None:
                display_label = f"SalesOrder {so_id}"
        elif label == "Delivery" and delivery_id is not None:
            display_label = f"Delivery {delivery_id}"
        elif label == "Product" and product_id is not None:
            display_label = f"Product {product_id}"

        # Build a compact hover title (limit keys for readability)
        title_lines = [f"<b>{_escape_html(display_label)}</b>"]
        # Prefer some common SAP identifiers if present
        preferred_keys = [
            "customerId",
            "salesOrderId",
            "salesOrderItemNo",
            "deliveryId",
            "billingDocumentId",
            "billingItemNo",
            "paymentId",
            "journalEntryId",
            "productId",
            "plant",
            "plantId",
            "addressId",
        ]
        used = set()
        for k in preferred_keys:
            if k in props and props[k] is not None:
                title_lines.append(f"{_escape_html(k)}: {_escape_html(str(props[k]))}")
                used.add(k)
        # Add a few additional properties if available
        extra_count = 0
        for k, v in props.items():
            if extra_count >= 10:
                break
            if k in used:
                continue
            if v is None or isinstance(v, (list, dict)):
                continue
            title_lines.append(f"{_escape_html(str(k))}: {_escape_html(str(v))}")
            extra_count += 1

        title = "<br>".join(title_lines)
        # Small dots, no on-canvas labels; borders match fill (no strong ring).
        if is_highlighted:
            node_size = 7
            node_opacity = 0.95
            node_color = {
                "background": NODE_HIGHLIGHT,
                "border": NODE_HIGHLIGHT,
                "highlight": {"background": NODE_HIGHLIGHT, "border": NODE_HIGHLIGHT},
            }
        else:
            node_size = 6
            node_opacity = 0.7
            node_color = {
                "background": NODE_DEFAULT,
                "border": NODE_DEFAULT,
                "highlight": {"background": NODE_HIGHLIGHT, "border": NODE_HIGHLIGHT},
            }

        net.add_node(
            node_id,
            label="",
            title=title,
            color=node_color,
            size=node_size,
            opacity=node_opacity,
            borderWidth=0,
            font={"size": 6, "color": "#64748b"},
        )

    for e in edges:
        src = str(e.get("from"))
        dst = str(e.get("to"))
        rel_type = e.get("type", "")
        props = e.get("properties", {}) or {}

        rel_title_lines = [f"<b>{_escape_html(rel_type)}</b>"]
        # Keep relationship hover info short
        rel_prop_count = 0
        for k, v in props.items():
            if rel_prop_count >= 6:
                break
            if v is None or isinstance(v, (list, dict)):
                continue
            rel_title_lines.append(f"{_escape_html(str(k))}: {_escape_html(str(v))}")
            rel_prop_count += 1
        rel_title = "<br>".join(rel_title_lines) if rel_prop_count else rel_type

        hl = node_is_highlighted.get(src) or node_is_highlighted.get(dst)
        edge_width = 0.9 if hl else 0.55
        net.add_edge(
            src,
            dst,
            title=rel_title,
            arrows="to",
            width=edge_width,
            color=EDGE_RGBA,
        )

    tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".html")
    tmp_file.close()
    net.save_graph(tmp_file.name)

    with open(tmp_file.name, "r", encoding="utf-8") as f:
        html_data = f.read()

    # Node click popup (injected JS into pyvis HTML)
    popup_div = """
    <div id="node-popup" style="
      position: fixed;
      top: 18px;
      right: 18px;
      width: 320px;
      max-width: 90vw;
      background: rgba(255,255,255,0.98);
      border: 1px solid #e5e7eb;
      border-radius: 14px;
      box-shadow: 0 10px 30px rgba(0,0,0,0.12);
      padding: 12px 14px;
      z-index: 9999;
      display: none;
      font-family: 'Inter', sans-serif;
      color: #0f172a;
    ">
      <div style="font-weight: 700; margin-bottom: 8px;">Node Details</div>
      <div id="node-popup-content" style="font-size: 13px;"></div>
    </div>
    """

    click_js = """
    <script>
      try {
        // pyvis creates a global `network` variable.
        network.on("click", function(params) {
          if (!params.nodes || params.nodes.length === 0) return;
          var nodeId = params.nodes[0];
          var node = network.body.data.nodes.get(nodeId);
          var popup = document.getElementById("node-popup");
          var content = document.getElementById("node-popup-content");
          if (!popup || !content || !node) return;

          var label = node.label || "Node";
          var title = node.title || "";
          content.innerHTML =
            "<div style='font-weight:600; margin-bottom:6px;'>" + label + "</div>" +
            "<div style='line-height:1.35;'>" + title + "</div>";
          popup.style.display = "block";
        });

        network.on("blur", function() {
          // hide popup when graph loses focus (minor UX)
        });

        // Hide popup when clicking on empty canvas
        network.on("click", function(params) {
          if (!params.nodes || params.nodes.length === 0) {
            var popup = document.getElementById("node-popup");
            if (popup) popup.style.display = "none";
          }
        });
      } catch (e) {
        // no-op
      }
    </script>
    """

    if "</body>" in html_data:
        html_data = html_data.replace("</body>", popup_div + click_js + "</body>")
    else:
        html_data = html_data + popup_div + click_js

    components.html(html_data, height=680, scrolling=False)


# ---------------- SESSION STATE ----------------
if "chat" not in st.session_state:
    st.session_state.chat = []
if "last_query" not in st.session_state:
    st.session_state.last_query = None
if "graph_data" not in st.session_state:
    st.session_state.graph_data = None
if "graph_highlight" not in st.session_state:
    st.session_state.graph_highlight = {
        "customerIds": [],
        "deliveryIds": [],
        "productIds": [],
        "salesOrderIds": [],
        "billingDocumentIds": [],
    }


if st.session_state.graph_data is None:
    try:
        st.session_state.graph_data = get_graph_data()
    except Exception as e:
        st.session_state.graph_data = {"nodes": [], "edges": []}
        st.warning(f"Unable to load graph from Neo4j: {e}")


left, right = st.columns([7, 3])


with left:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.subheader("Order to Cash Graph")
    render_graph(st.session_state.graph_data)
    st.markdown("</div>", unsafe_allow_html=True)


with right:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.subheader("Chat with Graph")

    if "chat" not in st.session_state:
        st.session_state.chat = []

    for msg in st.session_state.chat:
        with st.chat_message(msg["role"]):
            st.write(msg["text"])

    user_input = st.chat_input("Ask about Order to Cash...")

    if user_input:
        st.session_state.chat.append({"role": "user", "text": user_input})

        empty_highlight = {
            "customerIds": [],
            "deliveryIds": [],
            "productIds": [],
            "salesOrderIds": [],
            "billingDocumentIds": [],
        }

        if not is_valid_question(user_input):
            response = "Only SAP dataset questions allowed"
            st.session_state.graph_highlight = empty_highlight
        else:
            try:
                query = llm_to_query(user_input)
                st.session_state.last_query = query
                result = run_query(query)
                response = format_response(result, user_input)

                customer_ids = set()
                delivery_ids = set()
                product_ids = set()
                sales_order_ids = set()
                billing_document_ids = set()
                if isinstance(result, list):
                    for row in result:
                        if not isinstance(row, dict):
                            continue
                        if row.get("customerId") is not None:
                            customer_ids.add(str(row["customerId"]))
                        if row.get("deliveryId") is not None:
                            delivery_ids.add(str(row["deliveryId"]))
                        if row.get("productId") is not None:
                            product_ids.add(str(row["productId"]))
                        if row.get("salesOrderId") is not None:
                            sales_order_ids.add(str(row["salesOrderId"]))
                        if row.get("billingDocumentId") is not None:
                            billing_document_ids.add(str(row["billingDocumentId"]))
                        if row.get("billingDocument") is not None:
                            billing_document_ids.add(str(row["billingDocument"]))

                st.session_state.graph_highlight = {
                    "customerIds": sorted(customer_ids),
                    "deliveryIds": sorted(delivery_ids),
                    "productIds": sorted(product_ids),
                    "salesOrderIds": sorted(sales_order_ids),
                    "billingDocumentIds": sorted(billing_document_ids),
                }
            except Exception as e:
                st.session_state.last_query = None
                response = f"Neo4j error: {str(e)}"
                st.session_state.graph_highlight = empty_highlight

        st.session_state.chat.append({"role": "assistant", "text": response})

        gh = st.session_state.graph_highlight
        try:
            st.session_state.graph_data = get_graph_data(
                customerIds=gh["customerIds"] or None,
                deliveryIds=gh["deliveryIds"] or None,
                productIds=gh["productIds"] or None,
                salesOrderIds=gh["salesOrderIds"] or None,
                billingDocumentIds=gh.get("billingDocumentIds") or None,
            )
        except Exception as e:
            st.warning(f"Graph refresh failed: {e}")

    if st.session_state.chat:
        last_msg = st.session_state.chat[-1]
        if last_msg["role"] == "assistant":
            with st.chat_message("assistant"):
                st.write(last_msg["text"])

    if st.session_state.last_query:
        with st.expander("Generated Cypher Query"):
            st.code(st.session_state.last_query, language="cypher")

    st.markdown("</div>", unsafe_allow_html=True)