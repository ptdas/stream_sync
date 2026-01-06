import frappe
from stream_sync.stream_sync.doctype.stream_update_log.stream_update_log import make_stream_update_log

def test_manual_sync():
    doctype = "Sales Order"
    condition = {
        "docstatus": 1,
        # "custom_tax_status": "Tax",
        "amended_from": ["is", "set"]
    }
    
    data = frappe.db.get_all(doctype, filters=condition, pluck="name")
    
    for name in data:
        doc = frappe.get_doc(doctype, name)
        make_stream_update_log(doc, "Update")
        print(f"{name} - DONE")