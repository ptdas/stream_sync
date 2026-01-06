# Copyright (c) 2025, Jufer and contributors
# For license information, please see license.txt
import json
import time

import requests

import frappe
from frappe import _
from frappe.model.document import Document
from frappe.utils.data import get_link_to_form, get_url
from frappe.frappeclient import FrappeClient
from frappe.custom.doctype.custom_field.custom_field import create_custom_field
from frappe.utils.password import get_decrypted_password
from frappe.utils.background_jobs import get_jobs

class StreamProducer(Document):
	def before_insert(self):
		self.check_url()
		self.validate_stream_subscriber()
		self.incoming_change = True
		self.create_stream_consumer()
		self.create_custom_fields()

	def validate(self):
		self.validate_stream_subscriber()
		if frappe.flags.in_test:
			for entry in self.producer_doctypes:
				entry.status = "Actived"

	def validate_stream_subscriber(self):
		if not frappe.db.get_value("User", self.user, "api_key"):
			frappe.throw(
				_("Please generate keys for the Stream Subscriber User {0} first.").format(
					frappe.bold(get_link_to_form("User", self.user))
				)
			)

	def on_update(self):
		if not self.incoming_change:
			if frappe.db.exists("Stream Producer", self.name):
				if not self.api_key or not self.api_secret:
					frappe.throw(_("Please set API Key and Secret on the producer and consumer sites first."))
				else:
					doc_before_save = self.get_doc_before_save()
					if doc_before_save.api_key != self.api_key or doc_before_save.api_secret != self.api_secret:
						return

					self.update_stream_consumer()
					self.create_custom_fields()
		else:
			# when producer doc is updated it updates the consumer doc, set flag to avoid deadlock
			self.db_set("incoming_change", 0)
			self.reload()

	def on_trash(self):
		last_update = frappe.db.get_value("Stream Producer Last Update", dict(stream_producer=self.name))
		if last_update:
			frappe.delete_doc("Stream Producer Last Update", last_update)

	def check_url(self):
		valid_url_schemes = ("http", "https")
		frappe.utils.validate_url(self.producer_url, throw=True, valid_schemes=valid_url_schemes)

		# remove '/' from the end of the url like http://test_site.com/
		# to prevent mismatch in get_url() results
		if self.producer_url.endswith("/"):
			self.producer_url = self.producer_url[:-1]

	def create_stream_consumer(self):
		"""register Stream consumer on the producer site"""
		if self.is_producer_online():
			producer_site = FrappeClient(
				url=self.producer_url, api_key=self.api_key, api_secret=self.get_password("api_secret")
			)

			response = producer_site.post_api(
				"stream_sync.stream_sync.doctype.stream_consumer.stream_consumer.register_consumer",
				params={"data": json.dumps(self.get_request_data())},
			)
			if response:
				response = json.loads(response)
				self.set_last_update(response["last_update"])
			else:
				frappe.throw(
					_(
						"Failed to create an Stream Consumer or an Stream Consumer for the current site is already registered."
					)
				)

	def set_last_update(self, last_update):
		last_update_doc_name = frappe.db.get_value(
			"Stream Producer Last Update", dict(stream_producer=self.name)
		)
		if not last_update_doc_name:
			frappe.get_doc(
				dict(
					doctype="Stream Producer Last Update",
					stream_producer=self.producer_url,
					last_update=last_update,
				)
			).insert(ignore_permissions=True)
		else:
			frappe.db.set_value(
				"Stream Producer Last Update", last_update_doc_name, "last_update", last_update
			)

	def get_last_update(self):
		return frappe.db.get_value(
			"Stream Producer Last Update", dict(stream_producer=self.name), "last_update"
		)

	def get_request_data(self):
		consumer_doctypes = []
		for entry in self.producer_doctypes:
			if entry.has_mapping:
				# if mapping, subscribe to remote doctype on consumer's site
				dt = frappe.db.get_value("Doctype Mapping", entry.mapping, "remote_doctype")
			else:
				dt = entry.ref_doctype
			consumer_doctypes.append({
				"doctype": dt, 
				"condition": entry.condition,
				"stream_type": entry.stream_type,
				"target_docstatus": entry.target_docstatus,
				"amend_mode": entry.amend_mode,
				"unsubscribe": entry.unsubscribe,
				"inherit_condition": entry.inherit_condition,
			})	

		user_key = frappe.db.get_value("User", self.user, "api_key")
		user_secret = get_decrypted_password("User", self.user, "api_secret")
		return {
			"stream_consumer": get_url(),
			"consumer_doctypes": json.dumps(consumer_doctypes),
			"user": self.user,
			"api_key": user_key,
			"api_secret": user_secret,
		}

	def create_custom_fields(self):
		"""create custom field to store remote docname and remote site url"""
		for entry in self.producer_doctypes:
			if not entry.use_same_name:
				if not frappe.db.exists(
					"Custom Field", {"fieldname": "remote_docname", "dt": entry.ref_doctype}
				):
					df = dict(
						fieldname="remote_docname",
						label="Remote Document Name",
						fieldtype="Data",
						read_only=1,
						print_hide=1,
					)
					create_custom_field(entry.ref_doctype, df)
				if not frappe.db.exists(
					"Custom Field", {"fieldname": "remote_site_name", "dt": entry.ref_doctype}
				):
					df = dict(
						fieldname="remote_site_name",
						label="Remote Site",
						fieldtype="Data",
						read_only=1,
						print_hide=1,
					)
					create_custom_field(entry.ref_doctype, df)

	def update_stream_consumer(self):
		if self.is_producer_online():
			producer_site = get_producer_site(self.producer_url)
			stream_consumer = producer_site.get_doc("Stream Consumer", get_url())
			stream_consumer = frappe._dict(stream_consumer)
			if stream_consumer:
				config = stream_consumer.consumer_doctypes
				stream_consumer.consumer_doctypes = []
				for entry in self.producer_doctypes:
					if entry.has_mapping:
						# if mapping, subscribe to remote doctype on consumer's site
						ref_doctype = frappe.db.get_value("Doctype Mapping", entry.mapping, "remote_doctype")
					else:
						ref_doctype = entry.ref_doctype

					stream_consumer.consumer_doctypes.append(
						{
							"ref_doctype": ref_doctype,
							"status": get_approval_status(config, ref_doctype),
							"unsubscribe": entry.unsubscribe,
							"condition": entry.condition,
							"stream_type": entry.stream_type,
							"amend_mode": entry.amend_mode,
							"target_docstatus": entry.target_docstatus,
							"inherit_condition": entry.inherit_condition,
						}
					)
				stream_consumer.user = self.user
				stream_consumer.incoming_change = True
				producer_site.update(stream_consumer)

	def is_producer_online(self):
		"""check connection status for the Stream Producer site"""
		retry = 3
		while retry > 0:
			res = requests.get(self.producer_url)
			if res.status_code == 200:
				return True
			retry -= 1
			time.sleep(5)
		frappe.throw(_("Failed to connect to the Stream Producer site. Retry after some time."))


def get_producer_site(producer_url):
	"""create a FrappeClient object for Stream producer site"""
	producer_doc = frappe.get_doc("Stream Producer", producer_url)
	producer_site = FrappeClient(
		url=producer_url,
		api_key=producer_doc.api_key,
		api_secret=producer_doc.get_password("api_secret"),
	)
	return producer_site


def get_approval_status(config, ref_doctype):
	"""check the approval status for consumption"""
	for entry in config:
		if entry.get("ref_doctype") == ref_doctype:
			return entry.get("status")
	return "Pending"


@frappe.whitelist()
def pull_producer_data():
	"""Fetch data from producer node."""
	response = requests.get(get_url())
	if response.status_code == 200:
		for stream_producer in frappe.get_all("Stream Producer"):
			pull_from_node(stream_producer.name)
		return "success"
	return None


@frappe.whitelist()
def pull_from_node(stream_producer):
	"""pull all updates after the last update timestamp from Stream producer site"""
	stream_producer = frappe.get_doc("Stream Producer", stream_producer)
	producer_site = get_producer_site(stream_producer.producer_url)
	last_update = stream_producer.get_last_update()

	(doctypes, mapping_config, naming_config) = get_config(stream_producer.producer_doctypes)

	updates = get_updates(producer_site, last_update, doctypes)

	for update in updates:
		update.use_same_name = naming_config.get(update.ref_doctype)
		mapping = mapping_config.get(update.ref_doctype)
		if mapping:
			update.mapping = mapping
			update = get_mapped_update(update, producer_site)
		if not update.update_type == "Delete":
			update.data = json.loads(update.data)

		sync(update, producer_site, stream_producer)


def get_config(stream_config):
	"""get the doctype mapping and naming configurations for consumption"""
	doctypes, mapping_config, naming_config = [], {}, {}

	for entry in stream_config:
		if entry.status == "Actived":
			if entry.has_mapping:
				(mapped_doctype, mapping) = frappe.db.get_value(
					"Doctype Mapping", entry.mapping, ["remote_doctype", "name"]
				)
				mapping_config[mapped_doctype] = mapping
				naming_config[mapped_doctype] = entry.use_same_name
				doctypes.append(mapped_doctype)
			else:
				naming_config[entry.ref_doctype] = entry.use_same_name
				doctypes.append(entry.ref_doctype)
	return (doctypes, mapping_config, naming_config)


def sync(update, producer_site, stream_producer, in_retry=False):
	"""Sync the individual update"""
	try:
		if update.update_type == "Create":
			set_insert(update, producer_site, stream_producer.name)
		if update.update_type == "Update":
			set_update(update, producer_site, stream_producer.name)
		if update.update_type == "Delete":
			set_delete(update)
		if in_retry:
			return "Synced"
		log_stream_sync(update, stream_producer.name, "Synced")

	except Exception:
		if in_retry:
			if frappe.flags.in_test:
				print(frappe.get_traceback())
			return "Failed"
		log_stream_sync(update, stream_producer.name, "Failed", frappe.get_traceback())

	stream_producer.set_last_update(update.creation)
	frappe.db.commit()


def set_insert(update, producer_site, stream_producer):
	"""Sync insert type update"""
	if frappe.db.get_value(update.ref_doctype, update.docname):
		# doc already created
		return
	doc = frappe.get_doc(update.data)
	
	if update.mapping:
		if update.get("dependencies"):
			dependencies_created = sync_mapped_dependencies(update.dependencies, producer_site)
			for fieldname, value in dependencies_created.items():
				doc.update({fieldname: value})
	else:
		sync_dependencies(doc, producer_site, stream_producer)

	producers_doctype = frappe.db.get_value("Stream Producer Doctype", {"parent": stream_producer, "ref_doctype": update.ref_doctype}, "*", as_dict=True)

	doc.flags.ignore_permissions = True
	doc.flags.from_producer = True
	doc.flags.ignore_validate = producers_doctype.ignore_validate
	doc.flags.ignore_mandatory = producers_doctype.ignore_mandatory
	if producers_doctype.target_docstatus != "Follow Source":
		doc.docstatus = get_docstatus_target(producers_doctype.target_docstatus)

	if update.use_same_name:
		doc.insert(set_name=update.docname, set_child_names=False)
	else:
		# if Stream consumer is not saving documents with the same name as the producer
		# store the remote docname in a custom field for future updates
		doc.remote_docname = update.docname
		doc.remote_site_name = stream_producer
		doc.insert(set_child_names=False)


def set_update(update, producer_site, stream_producer):
	"""Sync update type update"""
	producers_doctype = frappe.db.get_value("Stream Producer Doctype", {"parent": stream_producer, "ref_doctype": update.ref_doctype}, "*", as_dict=True)
	if producers_doctype.amend_mode == "Update Source":
		docu = producer_site.get_doc(update.ref_doctype, update.docname)
		update.docname = check_amended_from(docu, producer_site)
		update.data.update({
			"name": update.docname,
			"amended_from": None
		})
	local_doc = get_local_doc(update)
	target_docstatus = get_docstatus_target(producers_doctype.target_docstatus)
	if local_doc and (local_doc.docstatus == target_docstatus or target_docstatus == 3):
		data = frappe._dict(update.data)

		if data.changed and producers_doctype.amend_mode != "Update Source":
			local_doc.update(data.changed)
		if data.removed and producers_doctype.amend_mode != "Update Source":
			local_doc = update_row_removed(local_doc, data.removed)
		if data.row_changed and producers_doctype.amend_mode != "Update Source":
			update_row_changed(local_doc, data.row_changed)
		if data.added and producers_doctype.amend_mode != "Update Source":
			local_doc = update_row_added(local_doc, data.added)
		if producers_doctype.amend_mode == "Update Source":
			local_doc = update_non_table_fields(local_doc, data)
			local_doc = replace_all_child_rows(local_doc, data)
			local_doc = mapping_data(local_doc, update.mapping)
		if update.mapping:
			if update.get("dependencies"):
				dependencies_created = sync_mapped_dependencies(update.dependencies, producer_site)
				for fieldname, value in dependencies_created.items():
					local_doc.update({fieldname: value})
			else:
				local_doc = update_non_table_fields(local_doc, data)
		else:
			sync_dependencies(local_doc, producer_site, stream_producer)
		local_doc.flags.ignore_validate = producers_doctype.ignore_validate
		local_doc.flags.ignore_version = True
		local_doc.flags.ignore_permission = True
		local_doc.save()
		local_doc.db_update_all()


def update_row_removed(local_doc, removed):
	"""Sync child table row deletion type update"""
	for tablename, rownames in removed.items():
		table = local_doc.get_table_field_doctype(tablename)
		for row in rownames:
			table_rows = local_doc.get(tablename)
			child_table_row = get_child_table_row(table_rows, row)
			table_rows.remove(child_table_row)
			local_doc.set(tablename, table_rows)
	return local_doc


def get_child_table_row(table_rows, row):
	for entry in table_rows:
		if entry.get("name") == row:
			return entry


def update_row_changed(local_doc, changed):
	"""Sync child table row updation type update"""
	for tablename, rows in changed.items():
		old = local_doc.get(tablename)
		for doc in old:
			for row in rows:
				if row["name"] == doc.get("name"):
					doc.update(row)


def update_row_added(local_doc, added):
	"""Sync child table row addition type update"""
	for tablename, rows in added.items():
		local_doc.extend(tablename, rows)
		for child in rows:
			child_doc = frappe.get_doc(child)
			child_doc.parent = local_doc.name
			child_doc.parenttype = local_doc.doctype
			child_doc.insert(set_name=child_doc.name)
	return local_doc


def set_delete(update):
	"""Sync delete type update"""
	local_doc = get_local_doc(update)
	if local_doc:
		local_doc.delete()


def get_updates(producer_site, last_update, doctypes):
	"""Get all updates generated after the last update timestamp"""
	docs = producer_site.post_request(
		{
			"cmd": "stream_sync.stream_sync.doctype.stream_update_log.stream_update_log.get_update_logs_for_consumer",
			"stream_consumer": get_url(),
			"doctypes": frappe.as_json(doctypes),
			"last_update": last_update,
		}
	)
	return [frappe._dict(d) for d in (docs or [])]


def get_local_doc(update):
	"""Get the local document if created with a different name"""
	try:
		if not update.use_same_name:
			return frappe.get_doc(update.ref_doctype, {"remote_docname": update.docname})

		return frappe.get_doc(update.ref_doctype, update.docname)
	except frappe.DoesNotExistError:
		return None


def sync_dependencies(document, producer_site, stream_producer):
	"""
	dependencies is a dictionary to store all the docs
	having dependencies and their sync status,
	which is shared among all nested functions.
	"""
	dependencies = {document: True}

	def check_doc_has_dependencies(doc, producer_site):
		"""Sync child table link fields first,
		then sync link fields,
		then dynamic links"""
		meta = frappe.get_meta(doc.doctype)
		table_fields = meta.get_table_fields()
		link_fields = meta.get_link_fields()
		dl_fields = meta.get_dynamic_link_fields()
		if table_fields:
			sync_child_table_dependencies(doc, table_fields, producer_site, stream_producer)
		if link_fields:
			sync_link_dependencies(doc, link_fields, producer_site)
		if dl_fields:
			sync_dynamic_link_dependencies(doc, dl_fields, producer_site)

	def sync_child_table_dependencies(doc, table_fields, producer_site, stream_producer):
		for df in table_fields:
			child_table = doc.get(df.fieldname)
			for entry in child_table:
				# child_doc = producer_site.get_doc(entry.doctype, entry.name)
				child_doc = get_doc_from_other_site(stream_producer, entry.doctype, entry.name)
				if child_doc:
					child_doc = frappe._dict(child_doc)
					set_dependencies(child_doc, frappe.get_meta(entry.doctype).get_link_fields(), producer_site)

	def sync_link_dependencies(doc, link_fields, producer_site):
		set_dependencies(doc, link_fields, producer_site)

	def sync_dynamic_link_dependencies(doc, dl_fields, producer_site):
		for df in dl_fields:
			docname = doc.get(df.fieldname)
			linked_doctype = doc.get(df.options)
			if docname and not check_dependency_fulfilled(linked_doctype, docname):
				master_doc = producer_site.get_doc(linked_doctype, docname)
				frappe.get_doc(master_doc).insert(set_name=docname)

	def set_dependencies(doc, link_fields, producer_site):
		for df in link_fields:
			docname = doc.get(df.fieldname)
			linked_doctype = df.get_link_doctype()
			if docname and not check_dependency_fulfilled(linked_doctype, docname):
				master_doc = producer_site.get_doc(linked_doctype, docname)
				try:
					master_doc = frappe.get_doc(master_doc)
					master_doc.insert(set_name=docname, ignore_permissions=True)
					frappe.db.commit()

				# for dependency inside a dependency
				except Exception:
					dependencies[master_doc] = True

	def check_dependency_fulfilled(linked_doctype, docname):
		return frappe.db.exists(linked_doctype, docname)

	while dependencies[document]:
		# find the first non synced dependency
		for item in reversed(list(dependencies.keys())):
			if dependencies[item]:
				dependency = item
				break

		check_doc_has_dependencies(dependency, producer_site)

		# mark synced for nested dependency
		if dependency != document:
			dependencies[dependency] = False
			dependency.insert()

		# no more dependencies left to be synced, the main doc is ready to be synced
		# end the dependency loop
		if not any(list(dependencies.values())[1:]):
			dependencies[document] = False


def sync_mapped_dependencies(dependencies, producer_site):
	dependencies_created = {}
	for entry in dependencies:
		doc = frappe._dict(json.loads(entry[1]))
		docname = frappe.db.exists(doc.doctype, doc.name)
		if not docname:
			doc = frappe.get_doc(doc).insert(set_child_names=False)
			dependencies_created[entry[0]] = doc.name
		else:
			dependencies_created[entry[0]] = docname

	return dependencies_created


def log_stream_sync(update, stream_producer, sync_status, error=None):
	"""Log stream update received with the sync_status as Synced or Failed"""
	doc = frappe.new_doc("Stream Sync Log")
	doc.update_type = update.update_type
	doc.ref_doctype = update.ref_doctype
	doc.status = sync_status
	doc.stream_producer = stream_producer
	doc.producer_doc = update.docname
	doc.data = frappe.as_json(update.data)
	doc.use_same_name = update.use_same_name
	doc.mapping = update.mapping if update.mapping else None
	if update.use_same_name:
		doc.docname = update.docname
	else:
		doc.docname = frappe.db.get_value(update.ref_doctype, {"remote_docname": update.docname}, "name")
	if error:
		doc.error = error
	doc.insert()


def get_mapped_update(update, producer_site):
	"""get the new update document with mapped fields"""
	mapping = frappe.get_doc("Doctype Mapping", update.mapping)
	if update.update_type == "Create":
		doc = frappe._dict(json.loads(update.data))
		mapped_update = mapping.get_mapping(doc, producer_site, update.update_type)
		update.data = mapped_update.get("doc")
		update.dependencies = mapped_update.get("dependencies", None)
	elif update.update_type == "Update":
		mapped_update = mapping.get_mapped_update(update, producer_site)
		update.data = mapped_update.get("doc")
		update.dependencies = mapped_update.get("dependencies", None)

	update["ref_doctype"] = mapping.local_doctype
	return update


@frappe.whitelist()
def new_stream_notification(producer_url):
	"""Pull data from producer when notified"""
	enqueued_method = "stream_sync.stream_sync.doctype.stream_producer.stream_producer.pull_from_node"
	jobs = get_jobs()
	if not jobs or enqueued_method not in jobs[frappe.local.site]:
		frappe.enqueue(enqueued_method, queue="default", **{"stream_producer": producer_url})


@frappe.whitelist()
def resync(update):
	"""Retry syncing update if failed"""
	update = frappe._dict(json.loads(update))
	producer_site = get_producer_site(update.stream_producer)
	stream_producer = frappe.get_doc("Stream Producer", update.stream_producer)
	if update.mapping:
		update = get_mapped_update(update, producer_site)
		update.data = json.loads(update.data)
	return sync(update, producer_site, stream_producer, in_retry=True)


def check_amended_from(doc, producer_site):
	if doc.get('amended_from'):
		amend_doc = producer_site.get_doc(doc.get('doctype'), doc.get('amended_from'))
		return check_amended_from(amend_doc, producer_site)
	return doc.get('name')


def replace_all_child_rows(local_doc, changed):
	"""Ganti semua child table rows dan insert ulang ke database.
	   Hapus dulu record lama berdasarkan parent agar tidak duplicate entry."""
	for tablename, rows in changed.items():
		# Lewati jika rows bukan list
		if not isinstance(rows, (list, tuple)):
			continue

		# Pastikan field adalah child table
		table_field = next(
			(f for f in local_doc.meta.fields if f.fieldtype == "Table" and f.fieldname == tablename),
			None
		)
		if not table_field:
			continue

		# Dapatkan nama doctype child-nya
		child_doctype = table_field.options

		# Hapus semua record lama di database untuk parent ini
		try:
			frappe.db.delete(child_doctype, {"parent": local_doc.name})
		except Exception as e:
			frappe.log_error(f"Error deleting old child rows for {child_doctype}: {str(e)}")
			continue

		# Kosongkan child table di memori
		local_doc.set(tablename, [])

		# Tambahkan dan insert setiap baris baru ke DB
		for row in rows:
			if not isinstance(row, dict):
				continue

			try:
				# Tambahkan ke child table di memori
				local_doc.append(tablename, row)

				# Insert langsung ke database
				child_doc = frappe.get_doc(row)
				child_doc.parent = local_doc.name
				child_doc.parenttype = local_doc.doctype
				child_doc.parentfield = tablename
				child_doc.insert(ignore_permissions=True, set_name=child_doc.name)
			except Exception as e:
				frappe.log_error(f"Error inserting child row in {child_doctype}: {str(e)}")
				continue

	return local_doc


def update_non_table_fields(local_doc, changed):
	"""Update only non-table fields in local_doc based on changed data, skipping system fields."""
	system_fields = {"creation", "modified","owner", "idx", "doctype", "name", "docstatus"}

	for fieldname, value in changed.items():
		# Lewati jika field adalah Table
		table_field = next(
			(f for f in local_doc.meta.fields if f.fieldtype == "Table" and f.fieldname == fieldname),
			None
		)
		if table_field:
			continue

		# Lewati field sistem yang dikontrol oleh Frappe
		if fieldname in system_fields:
			continue

		# Jika field ada di dokumen, ubah nilainya
		if fieldname in local_doc.as_dict():
			local_doc.set(fieldname, value)

	return local_doc

def mapping_data(local_doc, mapping_name):
	# kalau mappingnya kosong, retur

	if not mapping:
		return

	"""Lakukan mapping data ke local_doc secara rekursif berdasarkan Doctype Mapping"""
	mapping_doc = frappe.get_doc("Doctype Mapping", mapping_name)

	for m in mapping_doc.field_mapping:
		if not m.mapping_type or m.mapping_type == "":
			if getattr(m, "is_empty", False):
				value = None
			else:
				value = m.source_value or getattr(local_doc, m.local_fieldname, None)

			if hasattr(local_doc, m.local_fieldname):
				try:
					local_doc.set(m.local_fieldname, value)
				except Exception:
					setattr(local_doc, m.local_fieldname, value)

		elif m.mapping_type == "Child Table":
			if not m.local_fieldname or m.is_empty:
				continue
			if hasattr(local_doc, m.local_fieldname):
				child_table = getattr(local_doc, m.local_fieldname)
				if isinstance(child_table, list):
					for child_row in child_table:
						mapping_data(child_row, m.mapping)

		elif m.mapping_type == "Document":
			mapping_data(local_doc, m.mapping)

	return local_doc

def get_docstatus_target(target_docstatus):
	docstatus ={
		"Draft": 0,
		"Submitted": 1,
		"Cancelled":2,
		"Follow Source": 3
	}
	return docstatus[target_docstatus]

import frappe, re

def get_doc_from_other_site(site_url_or_name, doctype, docname):
	"""
	Mengambil dokumen dari site lain tanpa menggunakan FrappeClient.
	Fungsi ini langsung membaca database site lain menggunakan frappe.init() + frappe.connect().
	
	Args:
		site_url_or_name (str): Nama site atau URL site producer (misal: 'producer.site.com' atau 'https://producer.site.com')
		doctype (str): Nama Doctype dokumen yang ingin diambil
		docname (str): Nama dokumen (name) yang ingin diambil

	Returns:
		Document: frappe.model.document.Document dari site target
	"""
	current_site = frappe.local.site  # simpan site aktif sekarang
	try:
		# Hilangkan protokol jika ada
		site_name = re.sub(r"^https?://", "", site_url_or_name).strip("/")

		# Inisialisasi dan koneksi ke site target
		frappe.destroy()
		frappe.init(site_name)
		frappe.connect()

		# Abaikan permission
		frappe.flags.ignore_permissions = True

		# Ambil dokumen
		doc = frappe.db.get_value(doctype, docname, "*", as_dict=1)
		return doc

	except Exception as e:
		frappe.log_error(f"Error saat mengambil dokumen {doctype} - {docname} dari site {site_name}: {e}", "get_doc_from_other_site")
		raise

	finally:
		# Tutup koneksi site target
		frappe.destroy()

		# Kembalikan ke site semula
		frappe.init(current_site)
		frappe.connect()
