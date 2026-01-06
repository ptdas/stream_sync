// Copyright (c) 2025, Jufer and contributors
// For license information, please see license.txt

frappe.ui.form.on("Stream Sync Log", {
	refresh(frm) {
        if (frm.doc.status == "Failed") {
			frm.add_custom_button(__("Resync"), function () {
				frappe.call({
					method: "stream_sync.stream_sync.doctype.stream_producer.stream_producer.resync",
					args: {
						update: frm.doc,
					},
					callback: function (r) {
						if (r.message) {
							frappe.msgprint(r.message);
							frm.set_value("status", r.message);
							frm.save();
						}
					},
				});
			});
		}
	},
});
