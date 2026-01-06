import frappe

from erpnext.selling.doctype.customer.customer import Customer
from frappe.model.naming import set_name_by_naming_series, set_name_from_naming_options

class CustomerCustom(Customer):
	def autoname(self):
		cust_master_name = frappe.defaults.get_global_default("cust_master_name")
		if cust_master_name == "Customer Name":
			self.name = self.get_customer_name()
		elif cust_master_name == "Naming Series":
			set_name_by_naming_series(self)
		else:
			set_name_from_naming_options(frappe.get_meta(self.doctype).autoname, self)