import frappe
import requests
import os
from frappe.utils.backups import BackupGenerator
from frappe import _

@frappe.whitelist()
def upload_backup():
    """Generate a new backup and upload it to the local server"""
    if frappe.request.method == "GET":
        # Generate the backup
        backup = BackupGenerator(
            db_name=frappe.conf.db_name,
            user=frappe.conf.db_name,
            password=frappe.conf.db_password,
            db_host=frappe.conf.db_host or "127.0.0.1",
            db_port=frappe.conf.db_port or "3306",
            db_type="mariadb"
        )
        backup.get_backup()

         # Prepare response with backup and tar files
        with open(backup.backup_path_db, 'rb') as f:
            backup_content = f.read()

        with open(backup.backup_path_files, 'rb') as f:
            public_files_content = f.read()

        with open(backup.backup_path_private_files, 'rb') as f:
            private_files_content = f.read()

        return {
            'backup': {
                'filename': os.path.basename(backup.backup_path_db),
                'content': backup_content.decode('latin1')
            },
            'public_files': {
                'filename': os.path.basename(backup.backup_path_files),
                'content': public_files_content.decode('latin1')
            },
            'private_files': {
                'filename': os.path.basename(backup.backup_path_private_files),
                'content': private_files_content.decode('latin1')
            }
        }
    else:
        frappe.throw(_("Invalid request method"))
