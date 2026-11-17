import os
import glob
from datetime import datetime
import frappe
from flask import Response
from frappe import _
from frappe.utils.backups import BackupGenerator
from frappe.utils.response import send_private_file

CHUNK_SIZE = 50 * 1024 * 1024  # 50 MB


def cleanup_old_server_backups(keep_count=5):
    """
    Prune older backup sets on Frappe Cloud server disk to prevent disk space exhaustion.
    """
    try:
        backup_dir = frappe.get_site_path("private", "backups")
        if not os.path.exists(backup_dir):
            return

        db_files = sorted(
            glob.glob(os.path.join(backup_dir, "*-database.sql.gz")),
            key=os.path.getmtime,
            reverse=True,
        )

        if len(db_files) > keep_count:
            for old_db in db_files[keep_count:]:
                prefix = os.path.basename(old_db).split("-")[0]
                for file_path in glob.glob(os.path.join(backup_dir, f"{prefix}*")):
                    try:
                        os.remove(file_path)
                    except OSError:
                        pass
    except Exception as e:
        frappe.log_error(title="Cloud Backup Cleanup Error", message=str(e))


def _resolve_backup_file(filename, generate_if_missing=True):
    """
    Find the latest backup file on the server. If recently created (< 15 min),
    reuse it so chunk downloads do not re-run mysqldump on every single chunk.
    """
    backup_dir = frappe.get_site_path("private", "backups")

    if filename == "database.sql.gz":
        pattern = "*-database.sql.gz"
    elif filename == "files.tar":
        pattern = "*-files.tar"
    elif filename == "private-files.tar":
        pattern = "*-private-files.tar"
    else:
        pattern = os.path.basename(filename)

    matches = (
        sorted(
            glob.glob(os.path.join(backup_dir, pattern)),
            key=os.path.getmtime,
            reverse=True,
        )
        if os.path.exists(backup_dir)
        else []
    )

    if matches and os.path.exists(matches[0]):
        file_age_sec = (datetime.now() - datetime.fromtimestamp(os.path.getmtime(matches[0]))).total_seconds()
        # If created within the last 15 minutes, reuse the existing file
        if file_age_sec < 900 or not generate_if_missing:
            return matches[0]

    if generate_if_missing:
        backup = BackupGenerator(
            db_name=frappe.conf.db_name,
            user=frappe.conf.db_name,
            password=frappe.conf.db_password,
            db_host=frappe.conf.db_host or "127.0.0.1",
            db_port=frappe.conf.db_port or "3306",
            db_type="mariadb",
        )
        backup.get_backup()
        cleanup_old_server_backups(keep_count=5)

        if filename == "database.sql.gz":
            return backup.backup_path_db
        elif filename == "files.tar":
            return backup.backup_path_files
        elif filename == "private-files.tar":
            return backup.backup_path_private_files
        else:
            return matches[0] if matches else None

    return matches[0] if matches else None


@frappe.whitelist()
def get_backup_info(filename):
    """
    Return file size and other metadata for the backup file.
    Preserved for client scripts currently using the chunked downloader.
    """
    try:
        if filename not in ("database.sql.gz", "files.tar", "private-files.tar"):
            frappe.throw(_("Invalid filename requested"))

        file_path = _resolve_backup_file(filename, generate_if_missing=True)

        if not file_path or not os.path.exists(file_path):
            frappe.throw(_("File not found"), frappe.DoesNotExistError)

        file_size = os.path.getsize(file_path)
        frappe.logger("api").info(f"get_backup_info: {filename}, size: {file_size}, chunk: {CHUNK_SIZE}")

        return {
            "file_size": file_size,
            "chunk_size": CHUNK_SIZE,
        }
    except Exception as e:
        frappe.logger("api").error(f"Error during file size retrieval: {str(e)}")
        frappe.throw(_("An error occurred while retrieving file information"))


@frappe.whitelist()
def download_backup_chunk(filename, chunk_number):
    """
    Stream a specific chunk of the backup file to the client.
    Reuses the already-generated file so mysqldump is NOT executed on every chunk.
    """
    try:
        if filename not in ("database.sql.gz", "files.tar", "private-files.tar"):
            frappe.throw(_("Invalid filename requested"))

        # Reuse existing backup file (chunk downloads happen immediately after get_backup_info)
        file_path = _resolve_backup_file(filename, generate_if_missing=False)

        if not file_path or not os.path.exists(file_path):
            # Fallback if file not yet generated
            file_path = _resolve_backup_file(filename, generate_if_missing=True)

        if not file_path or not os.path.exists(file_path):
            frappe.throw(_("File not found"), frappe.DoesNotExistError)

        total_file_size = os.path.getsize(file_path)
        start_byte = int(chunk_number) * CHUNK_SIZE
        end_byte = min(start_byte + CHUNK_SIZE, total_file_size)

        def generate():
            with open(file_path, "rb") as f:
                f.seek(start_byte)
                remaining = end_byte - start_byte
                while remaining > 0:
                    read_len = min(65536, remaining)
                    chunk_data = f.read(read_len)
                    if not chunk_data:
                        break
                    yield chunk_data
                    remaining -= len(chunk_data)

        return Response(
            generate(),
            mimetype="application/octet-stream",
            headers={
                "Content-Disposition": f"attachment;filename={filename}.part{chunk_number}",
                "Content-Range": f"bytes {start_byte}-{end_byte}/{total_file_size}",
            },
        )
    except Exception as e:
        frappe.logger("api").error(f"Error during file streaming: {str(e)}")
        frappe.throw(_("An error occurred during file streaming"))


@frappe.whitelist()
def upload_backup():
    """
    Generate a new backup and return it.
    Maintains 100% backward compatibility for legacy clients (auto_download_backup, etc.)
    by providing 'content' while also including 'download_url' for streaming clients.
    """
    try:
        if hasattr(frappe.local, "request") and frappe.local.request and frappe.local.request.method != "GET":
            frappe.throw(_("Invalid request method"))

        frappe.logger("api").info("Starting backup generation")
        backup = BackupGenerator(
            db_name=frappe.conf.db_name,
            user=frappe.conf.db_name,
            password=frappe.conf.db_password,
            db_host=frappe.conf.db_host or "127.0.0.1",
            db_port=frappe.conf.db_port or "3306",
            db_type="mariadb",
        )
        backup.get_backup()

        cleanup_old_server_backups(keep_count=5)

        db_filename = os.path.basename(backup.backup_path_db) if backup.backup_path_db else ""
        files_filename = os.path.basename(backup.backup_path_files) if backup.backup_path_files else ""
        private_filename = os.path.basename(backup.backup_path_private_files) if backup.backup_path_private_files else ""

        method_prefix = "/api/method/cloud_backup.cloud_backup.cloud_backup.download_backup_file"

        backup_content = b""
        if backup.backup_path_db and os.path.exists(backup.backup_path_db):
            with open(backup.backup_path_db, "rb") as f:
                backup_content = f.read()

        public_files_content = b""
        if backup.backup_path_files and os.path.exists(backup.backup_path_files):
            with open(backup.backup_path_files, "rb") as f:
                public_files_content = f.read()

        private_files_content = b""
        if backup.backup_path_private_files and os.path.exists(backup.backup_path_private_files):
            with open(backup.backup_path_private_files, "rb") as f:
                private_files_content = f.read()

        return {
            "backup": {
                "filename": db_filename,
                "size": len(backup_content),
                "download_url": f"{method_prefix}?filename={db_filename}" if db_filename else "",
                "content": backup_content.decode("latin1"),
            },
            "public_files": {
                "filename": files_filename,
                "size": len(public_files_content),
                "download_url": f"{method_prefix}?filename={files_filename}" if files_filename else "",
                "content": public_files_content.decode("latin1"),
            },
            "private_files": {
                "filename": private_filename,
                "size": len(private_files_content),
                "download_url": f"{method_prefix}?filename={private_filename}" if private_filename else "",
                "content": private_files_content.decode("latin1"),
            },
        }
    except Exception as e:
        frappe.logger("api").error(f"Error during backup: {str(e)}")
        frappe.throw(_("An error occurred during backup generation"))


@frappe.whitelist()
def get_backup_stream():
    """
    Lightweight endpoint: generates a new backup and returns ONLY metadata + download URLs.
    Does NOT decode or return file contents in JSON, using ~0 extra RAM!
    """
    if hasattr(frappe.local, "request") and frappe.local.request:
        frappe.only_for(("System Manager", "Administrator"))
        if frappe.local.request.method != "GET":
            frappe.throw(_("Invalid request method"))

    backup = BackupGenerator(
        db_name=frappe.conf.db_name,
        user=frappe.conf.db_name,
        password=frappe.conf.db_password,
        db_host=frappe.conf.db_host or "127.0.0.1",
        db_port=frappe.conf.db_port or "3306",
        db_type="mariadb",
    )
    backup.get_backup()

    cleanup_old_server_backups(keep_count=5)

    db_filename = os.path.basename(backup.backup_path_db) if backup.backup_path_db else ""
    files_filename = os.path.basename(backup.backup_path_files) if backup.backup_path_files else ""
    private_filename = os.path.basename(backup.backup_path_private_files) if backup.backup_path_private_files else ""

    method_prefix = "/api/method/cloud_backup.cloud_backup.cloud_backup.download_backup_file"

    return {
        "status": "success",
        "backup": {
            "filename": db_filename,
            "size": os.path.getsize(backup.backup_path_db) if backup.backup_path_db and os.path.exists(backup.backup_path_db) else 0,
            "download_url": f"{method_prefix}?filename={db_filename}" if db_filename else "",
        },
        "public_files": {
            "filename": files_filename,
            "size": os.path.getsize(backup.backup_path_files) if backup.backup_path_files and os.path.exists(backup.backup_path_files) else 0,
            "download_url": f"{method_prefix}?filename={files_filename}" if files_filename else "",
        },
        "private_files": {
            "filename": private_filename,
            "size": os.path.getsize(backup.backup_path_private_files) if backup.backup_path_private_files and os.path.exists(backup.backup_path_private_files) else 0,
            "download_url": f"{method_prefix}?filename={private_filename}" if private_filename else "",
        },
    }


@frappe.whitelist()
def download_backup_file(filename):
    """
    Stream a backup file directly using Nginx X-Accel-Redirect.
    Zero Python RAM usage; fast and reliable.
    """
    if hasattr(frappe.local, "request") and frappe.local.request:
        frappe.only_for(("System Manager", "Administrator"))

    if not filename:
        frappe.throw(_("Filename is required"))

    # Strictly sanitize filename against directory traversal
    safe_filename = os.path.basename(filename)
    if safe_filename in (".", "..") or not safe_filename:
        frappe.throw(_("Invalid filename"))

    relative_path = os.path.join("backups", safe_filename)
    full_path = os.path.join(frappe.get_site_path("private"), relative_path)

    if not os.path.exists(full_path):
        frappe.throw(_("Backup file '{0}' not found on server").format(safe_filename), frappe.DoesNotExistError)

    return send_private_file(relative_path)
