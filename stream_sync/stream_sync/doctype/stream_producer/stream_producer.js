// Copyright (c) 2025, Jufer and contributors
// For license information, please see license.txt

frappe.ui.form.on("Stream Producer", {
	refresh(frm) {
        frm.set_query("ref_doctype", "producer_doctypes", function () {
			return {
				filters: {
					issingle: 0,
					istable: 0,
				},
			};
		});

		frm.set_indicator_formatter("status", function (doc) {
			let indicator = "orange";
			if (doc.status == "Actived") {
				indicator = "green";
			} else if (doc.status == "Suspend") {
				indicator = "red";
			}
			return indicator;
		});
	},
});
