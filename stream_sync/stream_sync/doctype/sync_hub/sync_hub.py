# Copyright (c) 2025, Jufer and contributors
# For license information, please see license.txt
import re
import json

import frappe
from frappe.model.document import Document
from frappe.utils import get_datetime
from frappe.utils.background_jobs import get_jobs

from stream_sync.stream_sync.doctype.stream_consumer.stream_consumer import get_consumer_site
from stream_sync.stream_sync.doctype.stream_update_log.stream_update_log import make_stream_update_log

class SyncHub(Document):
	@frappe.whitelist()
	def get_data(self):
		key = "item_code" if self.ref_doctype == "Item" else "name"
		consumer_doctype = frappe.db.get_all("Stream Consumer Doctype", filters={"ref_doctype": self.ref_doctype, "stream_type": "Manual"}, fields="*")

		documents = []
		for row in consumer_doctype:
			consumer_site = get_consumer_site(row.parent)

			filters, or_filters = parse_condition(row.condition)
			
			if self.is_filter:
				if self.ref_doctype in ["Sales Order", "Purchase Order"]:
					filters.append(['transaction_date', 'between', [self.from_date, self.to_date]])
				else:
					filters.append(['posting_date', 'between', [self.from_date, self.to_date]])

			documents = get_new_data_producer(self.ref_doctype, consumer_site, key, filters, or_filters, documents)

			documents = get_outdated_docs(self.ref_doctype, consumer_site, key, filters, documents, row)

		return documents


def parse_condition(condition):

	if not condition:
		return [], []

	condition = condition.replace("doc.", "").strip()
	filters, or_filters = [], []

	# Deteksi apakah menggunakan OR
	if " or " in condition.lower():
		parts = [p.strip() for p in re.split(r"\s+or\s+", condition, flags=re.IGNORECASE)]
		target = or_filters
	else:
		parts = [p.strip() for p in re.split(r"\s+and\s+", condition, flags=re.IGNORECASE)]
		target = filters

	for part in parts:
		m = re.match(r"(\w+)\s*(==|!=|>=|<=|>|<|like)\s*(.*)", part, re.IGNORECASE)
		if not m:
			continue

		field, op, val = m.groups()
		field, op = field.strip(), op.strip().lower()

		# Bersihkan tanda kutip jika ada
		val = val.strip().strip('"').strip("'")

		# Konversi angka jika bisa
		if val.isdigit():
			val = int(val)

		# Ganti '==' dengan '=' agar sesuai format frappe
		target.append([field, "=" if op == "==" else op, val])

	return filters, or_filters


def get_new_data_producer(doctype, consumer_site, key, filters, or_filters, documents):
	check_doctype = frappe.db.get_value("DocType", doctype, "*", as_dict=True)
	if check_doctype.is_submittable:
		filters.append(["amended_from", "is", "not set"])
	producer_data = frappe.db.get_all(doctype, filters=filters, or_filters=or_filters, fields=[key])

	if check_doctype.is_submittable:
		filters = [f for f in filters if f[0] != "docstatus"]

	consumer_data = consumer_site.get_list(doctype, filters=filters, fields=[key])

	name_sources = {i[key]: i for i in producer_data}
	name_targets = {i[key]: i for i in consumer_data}
	
	new_data = [name for name in name_sources if name not in name_targets]
	for new in new_data:
		documents.append({
			"document": new,
			"update_type": "Create"
		})

	return documents

def get_outdated_docs(doctype, consumer_site, key, filters, documents, consumer_doctype):
	"""Bandingkan dokumen yang di-amend di Producer dan Consumer.
	Jika consumer.modified < producer.modified → masukkan ke array hasil.
	"""
	check_doctype = frappe.db.get_value("DocType", doctype, "*", as_dict=True)
	fields = [key, "modified"]
	if check_doctype.is_submittable:
		filters.append(["amended_from", "is", "set"])
		filters.append(["docstatus", "=", 1])

		fields.append("amended_from")
	producer_amended = frappe.get_all(
		doctype,
		filters=filters,
		fields=fields
	)


	for p_doc in producer_amended:
		doc = frappe.get_doc(doctype, p_doc[key])
		amended_from = check_amended_from(doc) if consumer_doctype.amend_mode == "Update Source" else p_doc[key]
		consumer_doc = consumer_site.get_value(
			doctype,
			[key, "modified", "docstatus"],
			{key :amended_from}
		)
		
		if not consumer_doc:
			continue

		consumer_modified = get_datetime(consumer_doc["modified"])
		consumer_docstatus = get_docstatus_target(consumer_doctype.target_docstatus)
		producer_modified = get_datetime(p_doc.modified)

		if consumer_modified < producer_modified and (consumer_docstatus == 3 or consumer_docstatus  == consumer_doc["docstatus"]):
			documents.append({
				"document": p_doc[key],
				"update_type": "Update"
			})

	return documents

def check_amended_from(doc):
	if doc.get('amended_from'):
		amend_doc = frappe.get_doc(doc.get('doctype'), doc.get('amended_from'))
		return check_amended_from(amend_doc)
	return doc.get('name')


@frappe.whitelist()
def sync(data):
	data = json.loads(data)
	for row in data['sync_hub_document']:
		doc = frappe.get_doc(data['ref_doctype'], row['document'])
		make_stream_update_log(doc, row['update_type'])

		# enqueued_method = (
		# 	"stream_sync.stream_sync.doctype.stream_update_log.stream_update_log.make_stream_update_log"
		# )
		# jobs = get_jobs()
		# if not jobs or enqueued_method not in jobs[frappe.local.site]:
		# 	frappe.enqueue(
		# 		enqueued_method, doc=doc, update_type=row['update_type'] , queue="long", enqueue_after_commit=True
		# 	)
	return freeze_on_progress()

@frappe.whitelist()
def get_doctype_sync(doctype, txt, searchfield, start, page_len, filters):
	consumer_doctype = list(set(frappe.db.get_all("Stream Consumer Doctype", filters={"stream_type": "Manual", "status": "Actived"},pluck="ref_doctype")))
	
	query = """
		SELECT name AS value FROM `tabDocType`
		WHERE name IN %(name)s AND name LIKE %(txt)s
	"""
	cond = {
		'name': consumer_doctype,
		"txt": f"%{txt}%",
		"start": start,
		"page_len": page_len
	}
	results = []
	if consumer_doctype:
		results = frappe.db.sql(query, cond)
	return results

def get_docstatus_target(target_docstatus):
	docstatus ={
		"Draft": 0,
		"Submitted": 1,
		"Cancelled":2,
		"Follow Source": 3
	}
	return docstatus[target_docstatus]

def freeze_on_progress():
	enqueued_method = (
		"stream_sync.stream_sync.doctype.stream_update_log.stream_update_log.make_stream_update_log"
	)
	jobs = get_jobs()
	if jobs and enqueued_method in jobs[frappe.local.site]:
		return freeze_on_progress()
	return "Success"