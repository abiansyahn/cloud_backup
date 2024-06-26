import frappe
import os
from frappe.utils.backups import BackupGenerator
from frappe import _
from datetime import datetime, timedelta
from smbprotocol.connection import Connection
from smbprotocol.session import Session
from smbprotocol.tree import TreeConnect
from smbprotocol.open import Open, CreateDisposition, FileAttributes, FilePipePrinterAccessMask

def upload_backup():
    """Generate a new backup and upload it to the NAS"""
    # Get settings from Auto Upload Backup Settings
    settings = frappe.get_single('Auto Upload Backup Settings')

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

    # NAS configuration
    nas_server = settings.nas_server
    nas_share = settings.nas_share
    nas_username = settings.nas_username
    nas_password = settings.nas_password

    # Define the NAS directory
    backup_dir = f"\\\\{nas_server}\\{nas_share}\\live_backup"

    # Create the directory if it doesn't exist
    create_nas_directory(nas_server, nas_share, nas_username, nas_password, 'live_backup')

    # Save database backup
    save_file_to_nas(backup_dir, os.path.basename(backup.backup_path_db), backup.backup_path_db, nas_username, nas_password)

    # Save public_files.tar
    save_file_to_nas(backup_dir, os.path.basename(backup.backup_path_files), backup.backup_path_files, nas_username, nas_password)

    # Save private_files.tar
    save_file_to_nas(backup_dir, os.path.basename(backup.backup_path_private_files), backup.backup_path_private_files, nas_username, nas_password)

    # Update last backup time
    settings.last_backup_time = datetime.now()
    settings.save()
    frappe.db.commit()

def save_file_to_nas(directory, filename, local_file_path, nas_username, nas_password):
    file_path = f"{directory}\\{filename}"
    # Connect to NAS and save the file
    conn = Connection(uuid.uuid4(), frappe.conf.nas_server)
    conn.connect()
    session = Session(conn, nas_username, nas_password)
    session.connect()
    tree = TreeConnect(session, f"\\\\{frappe.conf.nas_server}\\{frappe.conf.nas_share}")
    tree.connect()
    with open(local_file_path, 'rb') as local_file:
        open_file = Open(tree, file_path, access_mask=FilePipePrinterAccessMask.FILE_WRITE_DATA,
                         create_disposition=CreateDisposition.FILE_OVERWRITE_IF,
                         file_attributes=FileAttributes.FILE_ATTRIBUTE_NORMAL)
        open_file.create()
        open_file.write(local_file.read())
        open_file.close()

def create_nas_directory(nas_server, nas_share, nas_username, nas_password, directory_name):
    # Connect to NAS and create the directory if it doesn't exist
    conn = Connection(uuid.uuid4(), nas_server)
    conn.connect()
    session = Session(conn, nas_username, nas_password)
    session.connect()
    tree = TreeConnect(session, f"\\\\{nas_server}\\{nas_share}")
    tree.connect()
    open_dir = Open(tree, directory_name, access_mask=FilePipePrinterAccessMask.FILE_WRITE_DATA,
                    create_disposition=CreateDisposition.FILE_OPEN_IF,
                    file_attributes=FileAttributes.FILE_ATTRIBUTE_DIRECTORY)
    open_dir.create()
    open_dir.close()

def get_last_backup_time():
    settings = frappe.get_single('Auto Upload Backup Settings')
    return settings.last_backup_time

def schedule_backup():
    settings = frappe.get_single('Auto Upload Backup Settings')
    download_time = settings.download_time

    # Convert download_time to seconds
    time_intervals = {
        '1 Week': 7 * 24 * 60 * 60,
        '1 Hour': 60 * 60,
        '2 Hour': 2 * 60 * 60,
        '4 Hour': 4 * 60 * 60,
        '6 Hour': 6 * 60 * 60,
        '8 Hour': 8 * 60 * 60,
        '12 Hour': 12 * 60 * 60,
        '1 Day': 24 * 60 * 60,
        '1 Month': 30 * 24 * 60 * 60
    }

    interval = time_intervals.get(download_time, 60 * 60)  # Default to 1 hour if not found
    last_backup_time = get_last_backup_time()

    if last_backup_time:
        last_backup_time = datetime.strptime(last_backup_time, '%Y-%m-%d %H:%M:%S.%f')
        next_run_time = last_backup_time + timedelta(seconds=interval)
    else:
        next_run_time = datetime.now()

    if datetime.now() >= next_run_time:
        upload_backup()

    # Schedule the next check in one hour
    next_check_time = datetime.now() + timedelta(hours=1)
    frappe.enqueue('cloud_backup.scheduler.schedule_backup', enqueue_after_commit=True, execute_after=next_check_time)

if __name__ == "__main__":
    schedule_backup()