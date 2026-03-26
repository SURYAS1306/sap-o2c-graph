# load_sap_o2c_to_neo4j.py
import json
from pathlib import Path
from typing import Dict, Any, Iterable, List, Optional

from neo4j import GraphDatabase

NEO4J_URI = "neo4j+s://47689a7a.databases.neo4j.io"
NEO4J_USERNAME = "47689a7a"
NEO4J_PASSWORD = "w_mSU0XvXhC4fkOQkgCez9oQlDwJ8IswSgrVxfk6WPw"

DATA_DIR = Path("./sap-o2c-data")
BATCH_SIZE = 1000


def norm_item_no(x: Any) -> Optional[str]:
    """
    Normalize SAP item numbers like '000010' -> '10'.
    Returns None if x is null/empty.
    """
    if x is None:
        return None
    s = str(x).strip()
    if s == "" or s.lower() == "none":
        return None
    try:
        return str(int(s))
    except ValueError:
        return s


def sanitize_properties(d: Dict[str, Any]) -> Dict[str, Any]:
    """
    Keep scalar / dict properties; drop lists for safer Neo4j property handling.
    """
    out = {}
    for k, v in d.items():
        if isinstance(v, (list, dict)):
            continue
        out[k] = v
    return out


def iter_jsonl_records(dir_path: Path) -> Iterable[Dict[str, Any]]:
    for file_path in sorted(dir_path.glob("*.jsonl")):
        with file_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                yield json.loads(line)


def iter_jsonl_batches(dir_path: Path, batch_size: int) -> Iterable[List[Dict[str, Any]]]:
    batch = []
    for rec in iter_jsonl_records(dir_path):
        batch.append(rec)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def ensure_constraints(tx):
    queries = [
        "CREATE CONSTRAINT customer_customerId_unique IF NOT EXISTS FOR (n:Customer) REQUIRE n.customerId IS UNIQUE",
        "CREATE CONSTRAINT address_addressId_unique IF NOT EXISTS FOR (n:Address) REQUIRE n.addressId IS UNIQUE",
        "CREATE CONSTRAINT salesOrder_salesOrderId_unique IF NOT EXISTS FOR (n:SalesOrder) REQUIRE n.salesOrderId IS UNIQUE",
        "CREATE CONSTRAINT salesOrderItem_salesOrderItemId_unique IF NOT EXISTS FOR (n:SalesOrderItem) REQUIRE n.salesOrderItemId IS UNIQUE",
        "CREATE CONSTRAINT delivery_deliveryId_unique IF NOT EXISTS FOR (n:Delivery) REQUIRE n.deliveryId IS UNIQUE",
        "CREATE CONSTRAINT deliveryItem_deliveryItemId_unique IF NOT EXISTS FOR (n:DeliveryItem) REQUIRE n.deliveryItemId IS UNIQUE",
        "CREATE CONSTRAINT billingDocument_billingDocumentId_unique IF NOT EXISTS FOR (n:BillingDocument) REQUIRE n.billingDocumentId IS UNIQUE",
        "CREATE CONSTRAINT billingItem_billingItemId_unique IF NOT EXISTS FOR (n:BillingItem) REQUIRE n.billingItemId IS UNIQUE",
        "CREATE CONSTRAINT payment_paymentId_unique IF NOT EXISTS FOR (n:Payment) REQUIRE n.paymentId IS UNIQUE",
        "CREATE CONSTRAINT journalEntry_journalEntryId_unique IF NOT EXISTS FOR (n:JournalEntry) REQUIRE n.journalEntryId IS UNIQUE",
        "CREATE CONSTRAINT product_productId_unique IF NOT EXISTS FOR (n:Product) REQUIRE n.productId IS UNIQUE",
        "CREATE CONSTRAINT plant_plantId_unique IF NOT EXISTS FOR (n:Plant) REQUIRE n.plantId IS UNIQUE"
    ]

    for q in queries:
        tx.run(q)


def merge_nodes(tx, label: str, key_prop: str, rows: List[Dict[str, Any]], props_field: str = "props"):
    """
    rows must be: { <key_prop>: ..., props: {...} }
    """
    q = f"""
    UNWIND $rows AS row
    MERGE (n:{label} {{{key_prop}: row.{key_prop}}})
    SET n += row.{props_field}
    """
    tx.run(q, rows=rows)


def main():
    if not DATA_DIR.exists():
        raise FileNotFoundError(f"DATA_DIR not found: {DATA_DIR.resolve()}")

    driver = GraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USERNAME, NEO4J_PASSWORD),
        encrypted=True
    )

    with driver.session() as session:
        session.execute_write(ensure_constraints)

        # ---- Products ----
        prod_dir = DATA_DIR / "products"
        for batch in iter_jsonl_batches(prod_dir, BATCH_SIZE):
            rows = []
            for r in batch:
                product = r.get("product")
                if product is None:
                    continue
                rows.append({
                    "productId": str(product),
                    "props": sanitize_properties(r),
                })
            if rows:
                session.execute_write(lambda tx: merge_nodes(tx, "Product", "productId", rows))

        # ---- Plants ----
        plants_dir = DATA_DIR / "plants"
        if plants_dir.exists():
            for batch in iter_jsonl_batches(plants_dir, BATCH_SIZE):
                rows = []
                for r in batch:
                    plant = r.get("plant")
                    if plant is None:
                        continue
                    rows.append({
                        "plantId": str(plant),
                        "props": sanitize_properties(r),
                    })
                if rows:
                    session.execute_write(lambda tx: merge_nodes(tx, "Plant", "plantId", rows))

        # ---- Addresses ----
        addr_dir = DATA_DIR / "business_partner_addresses"
        for batch in iter_jsonl_batches(addr_dir, BATCH_SIZE):
            rows = []
            for r in batch:
                addr_id = r.get("addressId")
                if addr_id is None:
                    continue
                rows.append({
                    "addressId": str(addr_id),
                    "props": sanitize_properties(r),
                })
            if rows:
                session.execute_write(lambda tx: merge_nodes(tx, "Address", "addressId", rows))

        # ---- Customers ----
        bp_dir = DATA_DIR / "business_partners"
        for batch in iter_jsonl_batches(bp_dir, BATCH_SIZE):
            rows = []
            for r in batch:
                cid = r.get("businessPartner")
                if cid is None:
                    continue
                rows.append({
                    "customerId": str(cid),
                    "props": sanitize_properties(r),
                })
            if rows:
                session.execute_write(lambda tx: merge_nodes(tx, "Customer", "customerId", rows))

        # ---- SalesOrders ----
        soh_dir = DATA_DIR / "sales_order_headers"
        for batch in iter_jsonl_batches(soh_dir, BATCH_SIZE):
            rows = []
            for r in batch:
                so = r.get("salesOrder")
                if so is None:
                    continue
                rows.append({
                    "salesOrderId": str(so),
                    "props": sanitize_properties(r),
                })
            if rows:
                session.execute_write(lambda tx: merge_nodes(tx, "SalesOrder", "salesOrderId", rows))

        # ---- SalesOrderItems ----
        soi_dir = DATA_DIR / "sales_order_items"
        for batch in iter_jsonl_batches(soi_dir, BATCH_SIZE):
            rows = []
            for r in batch:
                so = r.get("salesOrder")
                soi = r.get("salesOrderItem")
                if so is None or soi is None:
                    continue
                so_item_norm = norm_item_no(soi)
                if so_item_norm is None:
                    continue
                salesOrderItemId = f"{so}-{so_item_norm}"
                props = sanitize_properties(r)
                # Ensure normalized keys are present for later matching
                props["salesOrderId"] = str(so)
                props["salesOrderItemNo"] = so_item_norm
                props["material"] = r.get("material")
                rows.append({
                    "salesOrderItemId": salesOrderItemId,
                    "props": props,
                })
            if rows:
                session.execute_write(lambda tx: merge_nodes(tx, "SalesOrderItem", "salesOrderItemId", rows))

        # ---- Deliveries ----
        delh_dir = DATA_DIR / "outbound_delivery_headers"
        for batch in iter_jsonl_batches(delh_dir, BATCH_SIZE):
            rows = []
            for r in batch:
                doc = r.get("deliveryDocument")
                if doc is None:
                    continue
                rows.append({
                    "deliveryId": str(doc),
                    "props": sanitize_properties(r),
                })
            if rows:
                session.execute_write(lambda tx: merge_nodes(tx, "Delivery", "deliveryId", rows))

        # ---- DeliveryItems ----
        deli_dir = DATA_DIR / "outbound_delivery_items"
        for batch in iter_jsonl_batches(deli_dir, BATCH_SIZE):
            rows = []
            for r in batch:
                dd = r.get("deliveryDocument")
                ddi = r.get("deliveryDocumentItem")
                if dd is None or ddi is None:
                    continue
                ddi_norm = norm_item_no(ddi)
                if ddi_norm is None:
                    continue
                deliveryItemId = f"{dd}-{ddi_norm}"
                props = sanitize_properties(r)

                props["deliveryId"] = str(dd)
                props["deliveryItemNo"] = ddi_norm

                # Normalize reference item number too (used for DeliveryItem->Product via SO item)
                ref_sd = r.get("referenceSdDocument")
                ref_sdi = r.get("referenceSdDocumentItem")
                if ref_sd is not None and ref_sdi is not None:
                    ref_sdi_norm = norm_item_no(ref_sdi)
                    props["referenceSalesOrderId"] = str(ref_sd)
                    props["referenceSalesOrderItemNo"] = ref_sdi_norm

                rows.append({
                    "deliveryItemId": deliveryItemId,
                    "props": props,
                })
            if rows:
                session.execute_write(lambda tx: merge_nodes(tx, "DeliveryItem", "deliveryItemId", rows))

        # ---- BillingDocuments ----
        bdh_dir = DATA_DIR / "billing_document_headers"
        for batch in iter_jsonl_batches(bdh_dir, BATCH_SIZE):
            rows = []
            for r in batch:
                bd = r.get("billingDocument")
                if bd is None:
                    continue
                props = sanitize_properties(r)
                # Make join keys explicit for later MATCH/MERGE
                props["companyCode"] = r.get("companyCode")
                props["fiscalYear"] = r.get("fiscalYear")
                props["accountingDocument"] = r.get("accountingDocument")
                rows.append({
                    "billingDocumentId": str(bd),
                    "props": props,
                })
            if rows:
                session.execute_write(lambda tx: merge_nodes(tx, "BillingDocument", "billingDocumentId", rows))

        # ---- BillingItems ----
        bdi_dir = DATA_DIR / "billing_document_items"
        for batch in iter_jsonl_batches(bdi_dir, BATCH_SIZE):
            rows = []
            for r in batch:
                bd = r.get("billingDocument")
                bdi = r.get("billingDocumentItem")
                if bd is None or bdi is None:
                    continue
                bdi_norm = norm_item_no(bdi)
                if bdi_norm is None:
                    continue
                billingItemId = f"{bd}-{bdi_norm}"
                props = sanitize_properties(r)
                props["billingDocumentId"] = str(bd)
                props["billingItemNo"] = bdi_norm
                rows.append({
                    "billingItemId": billingItemId,
                    "props": props,
                })
            if rows:
                session.execute_write(lambda tx: merge_nodes(tx, "BillingItem", "billingItemId", rows))

        # ---- Payments ----
        pay_dir = DATA_DIR / "payments_accounts_receivable"
        for batch in iter_jsonl_batches(pay_dir, BATCH_SIZE):
            rows = []
            for r in batch:
                companyCode = r.get("companyCode")
                fiscalYear = r.get("fiscalYear")
                accountingDocument = r.get("accountingDocument")
                if companyCode is None or fiscalYear is None or accountingDocument is None:
                    continue
                paymentId = f"{companyCode}-{fiscalYear}-{accountingDocument}"
                props = sanitize_properties(r)
                props["companyCode"] = companyCode
                props["fiscalYear"] = fiscalYear
                props["accountingDocument"] = accountingDocument
                rows.append({
                    "paymentId": str(paymentId),
                    "props": props,
                })
            if rows:
                session.execute_write(lambda tx: merge_nodes(tx, "Payment", "paymentId", rows))

        # ---- JournalEntries (document-level from journal line table) ----
        je_dir = DATA_DIR / "journal_entry_items_accounts_receivable"
        for batch in iter_jsonl_batches(je_dir, BATCH_SIZE):
            rows = []
            for r in batch:
                companyCode = r.get("companyCode")
                fiscalYear = r.get("fiscalYear")
                accountingDocument = r.get("accountingDocument")
                if companyCode is None or fiscalYear is None or accountingDocument is None:
                    continue
                journalEntryId = f"{companyCode}-{fiscalYear}-{accountingDocument}"
                props = sanitize_properties(r)
                props["companyCode"] = companyCode
                props["fiscalYear"] = fiscalYear
                props["accountingDocument"] = accountingDocument
                rows.append({
                    "journalEntryId": str(journalEntryId),
                    "props": props,
                })
            if rows:
                session.execute_write(lambda tx: merge_nodes(tx, "JournalEntry", "journalEntryId", rows))

        # ----------------------------
        # Relationships (batch)
        # ----------------------------

        # Customer -> SalesOrder via soldToParty
        for batch in iter_jsonl_batches(soh_dir, BATCH_SIZE):
            rel_rows = []
            for r in batch:
                sold_to = r.get("soldToParty")
                so = r.get("salesOrder")
                if sold_to is None or so is None:
                    continue
                rel_rows.append({
                    "customerId": str(sold_to),
                    "salesOrderId": str(so),
                })
            if rel_rows:
                session.execute_write(lambda tx: tx.run("""
                    UNWIND $rows AS row
                    MERGE (c:Customer {customerId: row.customerId})
                    MERGE (so:SalesOrder {salesOrderId: row.salesOrderId})
                    MERGE (c)-[:PLACES]->(so)
                """, rows=rel_rows))

        # a) SalesOrder -> SalesOrderItem (SalesOrder.salesOrder = SalesOrderItem.salesOrder)
        for batch in iter_jsonl_batches(soi_dir, BATCH_SIZE):
            rel_rows = []
            for r in batch:
                so = r.get("salesOrder")
                soi = r.get("salesOrderItem")
                if so is None or soi is None:
                    continue
                so_item_norm = norm_item_no(soi)
                if so_item_norm is None:
                    continue
                rel_rows.append({
                    "salesOrderId": str(so),
                    "salesOrderItemId": f"{so}-{so_item_norm}",
                })
            if rel_rows:
                session.execute_write(lambda tx: tx.run("""
                    UNWIND $rows AS row
                    MERGE (so:SalesOrder {salesOrderId: row.salesOrderId})
                    MERGE (soi:SalesOrderItem {salesOrderItemId: row.salesOrderItemId})
                    MERGE (so)-[:HAS_ITEM]->(soi)
                """, rows=rel_rows))

        # 2) SalesOrder -> Delivery via outbound_delivery_items.referenceSdDocument (item-level reference)
        for batch in iter_jsonl_batches(deli_dir, BATCH_SIZE):
            rel_rows = []
            for r in batch:
                ref_sd = r.get("referenceSdDocument")
                deliveryDocument = r.get("deliveryDocument")
                if ref_sd is None or deliveryDocument is None:
                    continue
                rel_rows.append({
                    "salesOrderId": str(ref_sd),
                    "deliveryId": str(deliveryDocument),
                })
            if rel_rows:
                session.execute_write(lambda tx: tx.run("""
                    UNWIND $rows AS row
                    MERGE (so:SalesOrder {salesOrderId: row.salesOrderId})
                    MERGE (d:Delivery {deliveryId: row.deliveryId})
                    MERGE (so)-[:HAS_DELIVERY]->(d)
                """, rows=rel_rows))

        # b) Delivery -> DeliveryItem
        for batch in iter_jsonl_batches(deli_dir, BATCH_SIZE):
            rel_rows = []
            for r in batch:
                dd = r.get("deliveryDocument")
                ddi = r.get("deliveryDocumentItem")
                if dd is None or ddi is None:
                    continue
                ddi_norm = norm_item_no(ddi)
                if ddi_norm is None:
                    continue
                rel_rows.append({
                    "deliveryId": str(dd),
                    "deliveryItemId": f"{dd}-{ddi_norm}",
                })
            if rel_rows:
                session.execute_write(lambda tx: tx.run("""
                    UNWIND $rows AS row
                    MERGE (d:Delivery {deliveryId: row.deliveryId})
                    MERGE (di:DeliveryItem {deliveryItemId: row.deliveryItemId})
                    MERGE (d)-[:HAS_ITEM]->(di)
                """, rows=rel_rows))

        # c) BillingDocument -> BillingItem
        for batch in iter_jsonl_batches(bdi_dir, BATCH_SIZE):
            rel_rows = []
            for r in batch:
                bd = r.get("billingDocument")
                bdi = r.get("billingDocumentItem")
                if bd is None or bdi is None:
                    continue
                bdi_norm = norm_item_no(bdi)
                if bdi_norm is None:
                    continue
                rel_rows.append({
                    "billingDocumentId": str(bd),
                    "billingItemId": f"{bd}-{bdi_norm}",
                })
            if rel_rows:
                session.execute_write(lambda tx: tx.run("""
                    UNWIND $rows AS row
                    MERGE (bd:BillingDocument {billingDocumentId: row.billingDocumentId})
                    MERGE (bi:BillingItem {billingItemId: row.billingItemId})
                    MERGE (bd)-[:HAS_ITEM]->(bi)
                """, rows=rel_rows))

        # 4) Customer -> BillingDocument
        for batch in iter_jsonl_batches(bdh_dir, BATCH_SIZE):
            rel_rows = []
            for r in batch:
                sold_to = r.get("soldToParty")
                bd = r.get("billingDocument")
                if sold_to is None or bd is None:
                    continue
                rel_rows.append({
                    "customerId": str(sold_to),
                    "billingDocumentId": str(bd),
                })
            if rel_rows:
                session.execute_write(lambda tx: tx.run("""
                    UNWIND $rows AS row
                    MERGE (c:Customer {customerId: row.customerId})
                    MERGE (bd:BillingDocument {billingDocumentId: row.billingDocumentId})
                    MERGE (c)-[:HAS_BILLING]->(bd)
                """, rows=rel_rows))

        # 3) Optional: Customer -> Address
        bpa_dir = DATA_DIR / "business_partner_addresses"
        for batch in iter_jsonl_batches(bpa_dir, BATCH_SIZE):
            rel_rows = []
            for r in batch:
                bp = r.get("businessPartner")
                addr_id = r.get("addressId")
                if bp is None or addr_id is None:
                    continue
                rel_rows.append({
                    "customerId": str(bp),
                    "addressId": str(addr_id),
                })
            if rel_rows:
                session.execute_write(lambda tx: tx.run("""
                    UNWIND $rows AS row
                    MERGE (c:Customer {customerId: row.customerId})
                    MERGE (a:Address {addressId: row.addressId})
                    MERGE (c)-[:HAS_ADDRESS]->(a)
                """, rows=rel_rows))

        # Delivery -> BillingDocument via billing_document_items.referenceSdDocument
        for batch in iter_jsonl_batches(bdi_dir, BATCH_SIZE):
            rel_rows = []
            for r in batch:
                deliveryDocument = r.get("referenceSdDocument")  # predecessor SD doc (delivery)
                billingDocument = r.get("billingDocument")
                if deliveryDocument is None or billingDocument is None:
                    continue
                rel_rows.append({
                    "deliveryId": str(deliveryDocument),
                    "billingDocumentId": str(billingDocument),
                })
            if rel_rows:
                session.execute_write(lambda tx: tx.run("""
                    UNWIND $rows AS row
                    MERGE (d:Delivery {deliveryId: row.deliveryId})
                    MERGE (bd:BillingDocument {billingDocumentId: row.billingDocumentId})
                    MERGE (d)-[:HAS_BILLING]->(bd)
                """, rows=rel_rows))

        # BillingDocument -> Payment via (companyCode, fiscalYear, accountingDocument)
        for batch in iter_jsonl_batches(pay_dir, BATCH_SIZE):
            rel_rows = []
            for r in batch:
                companyCode = r.get("companyCode")
                fiscalYear = r.get("fiscalYear")
                accountingDocument = r.get("accountingDocument")
                if companyCode is None or fiscalYear is None or accountingDocument is None:
                    continue
                rel_rows.append({
                    "companyCode": companyCode,
                    "fiscalYear": fiscalYear,
                    "accountingDocument": accountingDocument,
                    "paymentId": f"{companyCode}-{fiscalYear}-{accountingDocument}",
                })
            if rel_rows:
                session.execute_write(lambda tx: tx.run("""
                    UNWIND $rows AS row
                    MATCH (bd:BillingDocument {
                      companyCode: row.companyCode,
                      fiscalYear: row.fiscalYear,
                      accountingDocument: row.accountingDocument
                    })
                    MERGE (p:Payment {
                      paymentId: row.paymentId,
                      companyCode: row.companyCode,
                      fiscalYear: row.fiscalYear,
                      accountingDocument: row.accountingDocument
                    })
                    MERGE (bd)-[:HAS_PAYMENT]->(p)
                """, rows=rel_rows))

        # BillingDocument -> JournalEntry via same accounting keys
        for batch in iter_jsonl_batches(je_dir, BATCH_SIZE):
            rel_rows = []
            for r in batch:
                companyCode = r.get("companyCode")
                fiscalYear = r.get("fiscalYear")
                accountingDocument = r.get("accountingDocument")
                if companyCode is None or fiscalYear is None or accountingDocument is None:
                    continue
                rel_rows.append({
                    "companyCode": companyCode,
                    "fiscalYear": fiscalYear,
                    "accountingDocument": accountingDocument,
                    "journalEntryId": f"{companyCode}-{fiscalYear}-{accountingDocument}",
                })
            if rel_rows:
                session.execute_write(lambda tx: tx.run("""
                    UNWIND $rows AS row
                    MATCH (bd:BillingDocument {
                      companyCode: row.companyCode,
                      fiscalYear: row.fiscalYear,
                      accountingDocument: row.accountingDocument
                    })
                    MERGE (je:JournalEntry {
                      journalEntryId: row.journalEntryId,
                      companyCode: row.companyCode,
                      fiscalYear: row.fiscalYear,
                      accountingDocument: row.accountingDocument
                    })
                    MERGE (bd)-[:HAS_JOURNAL_ENTRY]->(je)
                """, rows=rel_rows))

        # Items -> Product via material

        # SalesOrderItem -> Product
        for batch in iter_jsonl_batches(soi_dir, BATCH_SIZE):
            rel_rows = []
            for r in batch:
                so = r.get("salesOrder")
                soi = r.get("salesOrderItem")
                material = r.get("material")
                if so is None or soi is None or material is None:
                    continue
                soi_norm = norm_item_no(soi)
                if soi_norm is None:
                    continue
                rel_rows.append({
                    "salesOrderItemId": f"{so}-{soi_norm}",
                    "productId": str(material),
                })
            if rel_rows:
                session.execute_write(lambda tx: tx.run("""
                    UNWIND $rows AS row
                    MERGE (soi:SalesOrderItem {salesOrderItemId: row.salesOrderItemId})
                    MERGE (p:Product {productId: row.productId})
                    MERGE (soi)-[:REFERENCES_PRODUCT]->(p)
                """, rows=rel_rows))

        # BillingItem -> Product
        for batch in iter_jsonl_batches(bdi_dir, BATCH_SIZE):
            rel_rows = []
            for r in batch:
                bd = r.get("billingDocument")
                bdi = r.get("billingDocumentItem")
                material = r.get("material")
                if bd is None or bdi is None or material is None:
                    continue
                bdi_norm = norm_item_no(bdi)
                if bdi_norm is None:
                    continue
                rel_rows.append({
                    "billingItemId": f"{bd}-{bdi_norm}",
                    "productId": str(material),
                })
            if rel_rows:
                session.execute_write(lambda tx: tx.run("""
                    UNWIND $rows AS row
                    MERGE (bi:BillingItem {billingItemId: row.billingItemId})
                    MERGE (p:Product {productId: row.productId})
                    MERGE (bi)-[:REFERENCES_PRODUCT]->(p)
                """, rows=rel_rows))

        # DeliveryItem -> Product (indirect because outbound_delivery_items has no material column in this dataset)
        for batch in iter_jsonl_batches(deli_dir, BATCH_SIZE):
            rel_rows = []
            for r in batch:
                deliveryDocument = r.get("deliveryDocument")
                deliveryDocumentItem = r.get("deliveryDocumentItem")
                ref_sd = r.get("referenceSdDocument")
                ref_sdi = r.get("referenceSdDocumentItem")
                if (
                    deliveryDocument is None
                    or deliveryDocumentItem is None
                    or ref_sd is None
                    or ref_sdi is None
                ):
                    continue

                delivery_item_no = norm_item_no(deliveryDocumentItem)
                ref_sdi_norm = norm_item_no(ref_sdi)
                if delivery_item_no is None or ref_sdi_norm is None:
                    continue

                rel_rows.append({
                    "deliveryItemId": f"{deliveryDocument}-{delivery_item_no}",
                    "referenceSalesOrderItemId": f"{ref_sd}-{ref_sdi_norm}",
                })

            if rel_rows:
                session.execute_write(lambda tx: tx.run("""
                    UNWIND $rows AS row
                    MERGE (di:DeliveryItem {deliveryItemId: row.deliveryItemId})
                    MATCH (soi:SalesOrderItem {salesOrderItemId: row.referenceSalesOrderItemId})
                    MATCH (p:Product {productId: soi.material})
                    MERGE (di)-[:REFERENCES_PRODUCT]->(p)
                """, rows=rel_rows))

    driver.close()
    print("Neo4j load complete.")


if __name__ == "__main__":
    main()