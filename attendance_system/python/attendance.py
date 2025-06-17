import os
import sys
import calendar
import msvcrt # For Windows file locking
from datetime import datetime, timedelta, time
import logging

import gspread
from google.oauth2.service_account import Credentials
import mysql.connector
from mysql.connector import Error
from gspread_formatting import (
    CellFormat, Color, BooleanCondition, BooleanRule,
    ConditionalFormatRule, GridRange, get_conditional_format_rules,
    format_cell_range
)
from gspread.utils import rowcol_to_a1
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# --- Configuration and Setup ---

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Retrieve sensitive information from environment variables
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
SHEET_ID = os.getenv('SHEET_ID')

# Ensure all critical environment variables are loaded
if not all([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, SHEET_ID]):
    logger.error("Missing one or more environment variables. Please check your .env file.")
    sys.exit(1)

# Constants for timing (can also be moved to .env if preferred)
SIGNIN_ALLOWED_TIME = time(6, 0)
CUTOFF_TIME = time(16, 0)
LATE_SIGNIN_TIME = time(8, 30)
COOLDOWN_MINUTES = 0.3

# Constants for msvcrt locking modes (using integer values as fallback)
MSVCRT_LOCK_NB = 2 # Corresponds to msvcrt._LK_NBLCK (Non-blocking lock)

# File-based lock for preventing multiple script instances on Windows
LOCK_FILE_DIR = os.path.join(os.path.expanduser("~"), "AppData", "Local", "AttendanceScript")
LOCK_FILE = os.path.join(LOCK_FILE_DIR, 'attendance_script.lock')

# Ensure the lock file directory exists before attempting to create the lock file
try:
    if not os.path.exists(LOCK_FILE_DIR):
        os.makedirs(LOCK_FILE_DIR)
        logger.info(f"Created lock file directory: {LOCK_FILE_DIR}")
except OSError as e:
    logger.critical(f"Failed to create lock file directory {LOCK_FILE_DIR}. Please check permissions: {e}")
    sys.exit(1)

# In-memory store for last tap times (resets on script restart)
last_tap_times = {}

# --- Google Sheets Authentication and Helper Functions ---

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(gspread.exceptions.APIError),
        after=lambda retry_state: logger.warning(f"Retrying Google Sheets operation... attempt {retry_state.attempt_number}"))
def _gsheet_api_call(func, *args, **kwargs):
    """Wrapper to handle Google Sheets API calls with retry logic."""
    try:
        result = func(*args, **kwargs)
        logger.info(f"Google Sheets API call '{func.__name__}' successful.")
        return result
    except gspread.exceptions.APIError as e:
        logger.error(f"Google Sheets API Error during '{func.__name__}': {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during Google Sheets API call '{func.__name__}': {e}")
        raise

try:
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(os.path.join(os.path.dirname(__file__), "..", "credentials.json"), scopes=scopes)
    client = gspread.authorize(creds)
    workbook = _gsheet_api_call(client.open_by_key, SHEET_ID)
    logger.info("Successfully connected to Google Sheets.")
except Exception as e:
    logger.critical(f"Failed to authenticate with Google Sheets: {e}")
    sys.exit(1)

# --- MySQL Database Helper Functions ---

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(mysql.connector.errors.OperationalError),
        after=lambda retry_state: logger.warning(f"Retrying MySQL operation... attempt {retry_state.attempt_number}"))
def _mysql_db_call(func, *args, **kwargs):
    """Wrapper to handle MySQL DB calls with retry logic."""
    return func(*args, **kwargs)

def get_db_connection():
    """Establishes and returns a new MySQL database connection."""
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        if connection.is_connected():
            logger.info("Successfully connected to MySQL database.")
            return connection
    except Error as e:
        logger.error(f"Error connecting to MySQL database: {e}")
        return None

# --- Application Logic Functions ---

def acquire_lock():
    """Acquires a file-based lock for Windows."""
    lock_fd = None
    try:
        lock_fd = os.open(LOCK_FILE, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o666)
        msvcrt.locking(lock_fd, MSVCRT_LOCK_NB, 1) # Lock 1 byte
        logger.info(f"Script lock acquired successfully on {LOCK_FILE}.")
        return lock_fd
    except FileExistsError:
        logger.error(f"Another instance of the script is already running. Lock file exists: {LOCK_FILE}.")
        if lock_fd is not None:
            os.close(lock_fd)
        return None
    except OSError as e:
        logger.error(f"Failed to acquire script lock due to OS error (errno: {e.errno}): {e.strerror}.")
        if lock_fd is not None:
            os.close(lock_fd)
        return None
    except Exception as e:
        logger.critical(f"An unexpected error occurred while acquiring script lock: {e}")
        if lock_fd is not None:
            os.close(lock_fd)
        return None

def release_lock(lock_fd):
    """Releases the file-based lock for Windows and removes the lock file."""
    if lock_fd is not None:
        try:
            pass # Lock is released when FD is closed
        finally:
            try:
                os.close(lock_fd)
                logger.info("Lock file descriptor closed.")
            except Exception as e:
                logger.error(f"Error closing lock file descriptor for {LOCK_FILE}: {e}")
            try:
                if os.path.exists(LOCK_FILE):
                    os.remove(LOCK_FILE)
                    logger.info(f"Lock file removed: {LOCK_FILE}")
            except OSError as e:
                logger.error(f"Failed to remove lock file {LOCK_FILE}. It might still be in use or permissions issue: {e}")
            except Exception as e:
                logger.error(f"An unexpected error occurred while removing lock file {LOCK_FILE}: {e}")
    else:
        logger.info("No lock file descriptor to release.")

def apply_monthly_conditional_formatting(month_sheet, headers_len):
    """Applies conditional formatting rules to the monthly sheet."""
    rules = get_conditional_format_rules(month_sheet)
    rules.clear() # Clear existing rules to prevent duplicates on re-application

    full_range = f"B2:{rowcol_to_a1(50, headers_len)}" # Assuming max 50 rows for staff for formatting ease

    present_rule = ConditionalFormatRule(
        ranges=[GridRange.from_a1_range(full_range, month_sheet)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_EQ', ['Present']), format=CellFormat(backgroundColor=Color(0.8, 1, 0.8)))
    )
    absent_rule = ConditionalFormatRule(
        ranges=[GridRange.from_a1_range(full_range, month_sheet)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_EQ', ['Absent']), format=CellFormat(backgroundColor=Color(1, 0.8, 0.8)))
    )
    late_rule = ConditionalFormatRule(
        ranges=[GridRange.from_a1_range(full_range, month_sheet)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_EQ', ['Present(LATE)']), format=CellFormat(backgroundColor=Color(1, 0.9, 0.6)))
    )
    rules.append(present_rule)
    rules.append(absent_rule)
    rules.append(late_rule)
    _gsheet_api_call(rules.save)
    logger.info("Applied conditional formatting to Monthly Sheet.")

def apply_daily_conditional_formatting(daily_sheet):
    """Applies conditional formatting rules to the daily sheet."""
    daily_rules = get_conditional_format_rules(daily_sheet)
    daily_rules.clear() # Clear existing rules

    in_rule = ConditionalFormatRule(
        ranges=[GridRange.from_a1_range('C2:C100', daily_sheet)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_EQ', ['IN']), format=CellFormat(backgroundColor=Color(0.8, 1, 0.8)))
    )
    out_rule = ConditionalFormatRule(
        ranges=[GridRange.from_a1_range('C2:C100', daily_sheet)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_EQ', ['OUT']), format=CellFormat(backgroundColor=Color(1, 0.8, 0.8)))
    )
    daily_rules.append(in_rule)
    daily_rules.append(out_rule)
    _gsheet_api_call(daily_rules.save)
    logger.info("Applied conditional formatting to Daily Sheet.")


def initialize_sheets(workbook, connection, today):
    """
    Initializes and updates the monthly and daily Google Sheets.
    Returns (month_sheet, daily_sheet, today_col_index, names_in_month_sheet, staff_name_to_daily_row_index)
    """
    year = today.year
    month = today.month
    num_days = calendar.monthrange(year, month)[1]
    date_headers = [f"{year}-{month:02d}-{day:02d}" for day in range(1, num_days + 1)]
    monthly_headers = ["Name"] + date_headers
    today_str = today.strftime("%Y-%m-%d")

    cursor = connection.cursor(dictionary=True, buffered=True)
    _mysql_db_call(cursor.execute, "SELECT tag_id, Name, department FROM Staff")
    staff_users = cursor.fetchall()
    cursor.close()
    
    # Pre-calculate staff name to department for daily sheet population
    staff_name_to_department = {staff['Name']: staff['department'] for staff in staff_users}

    # --- Monthly Sheet Setup ---
    try:
        month_sheet = _gsheet_api_call(workbook.worksheet, today.strftime("%B %Y"))
        logger.info(f"Using existing monthly sheet: {today.strftime('%B %Y')}")
    except gspread.exceptions.WorksheetNotFound:
        logger.info(f"Creating new monthly sheet: {today.strftime('%B %Y')}")
        month_sheet = _gsheet_api_call(workbook.add_worksheet, title=today.strftime("%B %Y"), rows="50", cols=str(len(monthly_headers)))
        _gsheet_api_call(month_sheet.update, "A1", [monthly_headers])
        # Apply conditional formatting ONLY when creating the sheet
        _gsheet_api_call(apply_monthly_conditional_formatting, month_sheet, len(monthly_headers))

    # Ensure monthly sheet headers are up-to-date (less frequent, but good to have)
    current_month_headers = _gsheet_api_call(month_sheet.row_values, 1)
    if current_month_headers != monthly_headers:
        logger.info("Updating monthly sheet headers.")
        _gsheet_api_call(month_sheet.update, "A1", [monthly_headers])
        current_month_headers = _gsheet_api_call(month_sheet.row_values, 1) # Re-fetch after update
        # Re-apply formatting if headers change, though ideally monthly sheets are fixed once created
        _gsheet_api_call(apply_monthly_conditional_formatting, month_sheet, len(monthly_headers))

    today_col_index = None
    try:
        today_col_index = current_month_headers.index(today_str) + 1
    except ValueError:
        logger.error(f"Critical Error: Today's date column '{today_str}' not found in monthly sheet headers. Exiting.")
        sys.exit(1)

    # Cache names from monthly sheet to avoid repeated API calls
    names_in_month_sheet = _gsheet_api_call(month_sheet.col_values, 1)
    existing_staff_names_in_month_sheet = set(names_in_month_sheet)
    
    updates_monthly_absent = []
    new_staff_rows_monthly = []

    # First pass: Identify new staff and pre-fill "Absent" for today
    for staff in staff_users:
        if staff['Name'] not in existing_staff_names_in_month_sheet:
            # New staff: add to a list for appending
            new_staff_rows_monthly.append([staff['Name']] + [''] * (len(monthly_headers) - 1))
            logger.info(f"Identified new staff member '{staff['Name']}' for monthly sheet.")
            # Add to the set immediately so we don't try to add them again later in this run
            existing_staff_names_in_month_sheet.add(staff['Name'])

    # Append new staff to monthly sheet in one go
    if new_staff_rows_monthly:
        _gsheet_api_call(month_sheet.append_rows, new_staff_rows_monthly)
        logger.info(f"Added {len(new_staff_rows_monthly)} new staff members to monthly sheet.")
        # Re-fetch names AFTER appending, to get correct row indices for new staff
        names_in_month_sheet = _gsheet_api_call(month_sheet.col_values, 1)


    # Second pass: Mark users as 'Absent' if not already marked for today
    # Build a more efficient name to row_index mapping
    staff_name_to_month_row_index = {name: i + 1 for i, name in enumerate(names_in_month_sheet)}

    current_day_values = _gsheet_api_call(month_sheet.col_values, today_col_index) # Get all values for today's column

    for staff_name in staff_name_to_month_row_index:
        row_index = staff_name_to_month_row_index[staff_name]
        if row_index >= len(current_day_values) + 1: # If this row was just appended and current_day_values hasn't caught up
            current_value = "" # Treat as empty
        else:
            current_value = current_day_values[row_index - 1] # -1 because list is 0-indexed

        if not current_value or current_value.strip().lower() == "":
            cell_address = rowcol_to_a1(row_index, today_col_index)
            updates_monthly_absent.append({"range": cell_address, "values": [["Absent"]]})
            
    if updates_monthly_absent:
        _gsheet_api_call(month_sheet.batch_update, updates_monthly_absent)
        logger.info(f"Marked {len(updates_monthly_absent)} new/unmarked users as 'Absent' for {today_str} in Monthly Sheet.")


    # --- Daily Sheet Setup ---
    try:
        daily_sheet = _gsheet_api_call(workbook.worksheet, today_str)
        logger.info(f"Using existing daily sheet tab: {today_str}")
    except gspread.exceptions.WorksheetNotFound:
        # If the daily sheet doesn't exist, rename the first default sheet
        daily_sheet = _gsheet_api_call(workbook.worksheets)[0]
        _gsheet_api_call(daily_sheet.update_title, today_str)
        logger.info(f"Renamed first sheet tab to daily: {today_str}")
        _gsheet_api_call(daily_sheet.clear) # Clear any previous content
        # Apply conditional formatting ONLY when creating the sheet
        _gsheet_api_call(apply_daily_conditional_formatting, daily_sheet)


    # Populate Daily Sheet with staff if empty or needs update
    expected_daily_headers = ["Name", "Department", "Status", "Time"]
    current_daily_headers = _gsheet_api_call(daily_sheet.row_values, 1) # Re-fetch to be sure

    if current_daily_headers != expected_daily_headers:
        logger.info("Updating daily sheet headers.")
        _gsheet_api_call(daily_sheet.update, "A1:D1", [expected_daily_headers])
        header_format = CellFormat(
            backgroundColor=Color(0.9, 0.9, 0.9),
            textFormat={"bold": True, "fontSize": 12}
        )
        format_cell_range(daily_sheet, 'A1:D1', header_format)
        current_daily_headers = expected_daily_headers # Update for subsequent checks
        # Re-apply formatting if headers change
        _gsheet_api_call(apply_daily_conditional_formatting, daily_sheet)
    
    # Synchronize staff in daily sheet
    existing_daily_names_in_sheet = set(_gsheet_api_call(daily_sheet.col_values, 1))
    daily_data_to_add = []
    
    for staff in staff_users:
        if staff['Name'] not in existing_daily_names_in_sheet:
            status = "OUT" # Default new staff to OUT for the daily sheet
            daily_data_to_add.append([staff['Name'], staff_name_to_department.get(staff['Name'], 'N/A'), status, ""])
            existing_daily_names_in_sheet.add(staff['Name']) # Add to set to avoid duplicates in this run

    if daily_data_to_add:
        _gsheet_api_call(daily_sheet.append_rows, daily_data_to_add)
        logger.info(f"Added {len(daily_data_to_add)} new staff members to daily sheet.")
    else:
        logger.info("Daily sheet already populated and up-to-date with staff data (or no new staff).")

    # Final read of daily names and mapping for runtime efficiency
    names_in_daily_sheet = _gsheet_api_call(daily_sheet.col_values, 1)
    staff_name_to_daily_row_index = {name: i + 1 for i, name in enumerate(names_in_daily_sheet)}

    return month_sheet, daily_sheet, today_col_index, staff_name_to_month_row_index, staff_name_to_daily_row_index


def get_assigned_name_and_tag(cursor, tag_id, connection_for_insert):
    """
    Fetches the name assigned to a given tag ID from the 'Sign' table.
    If not found in 'Sign', it checks 'Staff' and, if present,
    adds the staff member to the 'Sign' table automatically.
    Returns (assigned_name, tag_id_from_staff) if successful, (None, None) otherwise.
    """
    # 1. First, attempt to get the assigned name from the 'sign' table
    _mysql_db_call(cursor.execute, "SELECT Name FROM sign WHERE tag_id = %s", (tag_id,))
    sign_result = cursor.fetchone()
    
    if sign_result:
        assigned_name = sign_result['Name']
        # Verify tag_id still exists in Staff table for consistency check
        # This second query is fast as it's indexed and returns a single row.
        _mysql_db_call(cursor.execute, "SELECT tag_id FROM Staff WHERE tag_id = %s", (tag_id,))
        staff_verification_result = cursor.fetchone()
        if not staff_verification_result:
            logger.warning(f"Tag ID '{tag_id}' found in 'Sign' but not in 'Staff'. Data inconsistency.")
            return None, None
        logger.info(f"Tag ID '{tag_id}' found in 'Sign' table for '{assigned_name}'.")
        return assigned_name, tag_id
    
    # 2. If tag_id not found in 'Sign' table, check 'Staff' table
    logger.info(f"Tag ID '{tag_id}' not found in 'Sign' table. Checking 'Staff' table...")
    _mysql_db_call(cursor.execute, "SELECT Name FROM Staff WHERE tag_id = %s", (tag_id,))
    staff_result = cursor.fetchone()

    if staff_result:
        staff_name = staff_result['Name']
        logger.info(f"Tag ID '{tag_id}' found in 'Staff' table for '{staff_name}'. Automatically adding to 'Sign' table.")
        
        # Add the staff member to the 'sign' table
        try:
            insert_cursor = connection_for_insert.cursor() # Use a new cursor for the insert
            _mysql_db_call(insert_cursor.execute,
                           "INSERT INTO sign (tag_id, Name) VALUES (%s, %s)",
                           (tag_id, staff_name))
            connection_for_insert.commit()
            insert_cursor.close()
            logger.info(f"Successfully added '{staff_name}' (Tag: {tag_id}) to 'Sign' table.")
            return staff_name, tag_id
        except Error as e:
            logger.error(f"Error adding '{staff_name}' (Tag: {tag_id}) to 'Sign' table: {e}")
            connection_for_insert.rollback()
            return None, None
    else:
        # 3. If tag_id not found in 'Staff' either
        logger.warning(f"Tag ID '{tag_id}' not found in 'Sign' or 'Staff' tables. Cannot process.")
        return None, None


def get_current_onsite_status(cursor, tag_id, today_start, today_end):
    """
    Checks the latest 'onsite' record for a tag_id within today's range
    to determine current IN/OUT status (Active column) and gets the onsite_id.
    Returns (current_status_active, onsite_id) or (None, None) if no relevant record.
    """
    _mysql_db_call(cursor.execute, """
        SELECT onsite_id, Active
        FROM onsite
        WHERE tag_id = %s AND scan_date BETWEEN %s AND %s
        ORDER BY scan_date DESC
        LIMIT 1
    """, (tag_id, today_start, today_end))
    result = cursor.fetchone()
    if result:
        return result['Active'], result['onsite_id']
    return None, None # No record found for today

def add_onsite_record_to_db(connection, cursor, tag_id, now):
    """
    Adds a new 'IN' record to the 'onsite' database table.
    Sets 'Active' to 1 (IN) and 'sign_out_date' to NULL.
    """
    try:
        _mysql_db_call(cursor.execute, """
            INSERT INTO onsite (tag_id, scan_date, Active, sign_out_date)
            VALUES (%s, %s, %s, %s)
        """, (tag_id, now, 1, None)) # Active = 1 for IN
        connection.commit()
        logger.info(f"Tag '{tag_id}' marked IN in onsite DB at {now}.")
        return True
    except Error as e:
        logger.error(f"Error marking tag '{tag_id}' IN in DB: {e}")
        connection.rollback() # Rollback on error
        return False

def update_onsite_record_in_db(connection, cursor, onsite_id, now):
    """
    Updates an existing 'onsite' record to mark a user as 'OUT'.
    Sets 'Active' to 0 (OUT) and updates 'sign_out_date'.
    """
    try:
        _mysql_db_call(cursor.execute, """
            UPDATE onsite
            SET Active = %s, sign_out_date = %s
            WHERE onsite_id = %s
        """, (0, now, onsite_id)) # Active = 0 for OUT
        connection.commit()
        logger.info(f"Onsite record {onsite_id} updated to OUT in onsite DB at {now}.")
        return True
    except Error as e:
        logger.error(f"Error marking onsite record {onsite_id} OUT in DB: {e}")
        connection.rollback() # Rollback on error
        return False

def get_staff_name_by_tag_id(cursor, tag_id):
    """Fetches the staff name given a tag ID from the Staff table."""
    _mysql_db_call(cursor.execute, "SELECT Name FROM Staff WHERE tag_id = %s", (tag_id,))
    result = cursor.fetchone()
    return result['Name'] if result else None

def update_monthly_sheet_status(month_sheet, assigned_name, staff_name_to_month_row_index, today_col_index, now_time, current_db_status_active):
    """
    Updates the monthly Google Sheet with the user's presence status based on the latest DB action.
    `current_db_status_active`: 1 if the user just signed IN, 0 if just signed OUT.
    """
    row_index = staff_name_to_month_row_index.get(assigned_name)
    if not row_index:
        logger.warning(f"Staff member '{assigned_name}' not found in monthly sheet name cache. Cannot update status.")
        return

    cell = rowcol_to_a1(row_index, today_col_index)
    
    new_status = ""
    if current_db_status_active == 1: # Just signed IN
        new_status = "Present(LATE)" if now_time > LATE_SIGNIN_TIME else "Present"
        # Using batch_update for a single cell is slightly less efficient than update_acell,
        # but makes it easier to expand to multiple updates if needed.
        _gsheet_api_call(month_sheet.batch_update, [{"range": cell, "values": [[new_status]]}])
        logger.info(f"Monthly Sheet: '{assigned_name}' updated to '{new_status}' (signed IN).")
    elif current_db_status_active == 0: # Just signed OUT
        logger.info(f"Monthly Sheet: '{assigned_name}' status (Present/Late) remains unchanged after sign-out.")
    else:
        logger.warning(f"Monthly Sheet: Unexpected `current_db_status_active` value: {current_db_status_active}. No update performed.")


def is_after_cutoff(current_time):
    """Checks if the current time is after the defined cutoff time."""
    return current_time > CUTOFF_TIME

def is_signin_allowed(current_time):
    """Checks if sign-in is allowed based on defined time window."""
    return SIGNIN_ALLOWED_TIME <= current_time <= CUTOFF_TIME

def update_daily_sheet_row(daily_sheet, user_name, new_status, current_time_str, staff_name_to_daily_row_index):
    """Updates user status (IN/OUT) and time in Daily Sheet."""
    row = staff_name_to_daily_row_index.get(user_name)
    if not row:
        logger.warning(f"Daily Sheet: '{user_name}' not found in daily sheet name cache. Cannot update status.")
        return False

    updates = []
    updates.append({"range": rowcol_to_a1(row, 3), "values": [[new_status]]}) # Status column
    updates.append({"range": rowcol_to_a1(row, 4), "values": [[current_time_str]]}) # Time column
    _gsheet_api_call(daily_sheet.batch_update, updates)

    logger.info(f"Daily Sheet: '{user_name}' set to {new_status} at {current_time_str}.")
    return True

def check_cooldown(user_name):
    """Checks if a user is within the cooldown period after their last tap."""
    now = datetime.now()
    if user_name in last_tap_times:
        elapsed = now - last_tap_times[user_name]
        if elapsed < timedelta(minutes=COOLDOWN_MINUTES):
            remaining = COOLDOWN_MINUTES - elapsed.total_seconds() / 60
            logger.warning(f"DENIED for {user_name}: Wait {remaining:.1f} minutes before next tap.")
            return False
    return True

def on_tag_scan(tag_id, connection, month_sheet, daily_sheet, today_col_index, staff_name_to_month_row_index, staff_name_to_daily_row_index):
    """Processes an RFID tag scan event."""
    now = datetime.now()
    current_time = now.time()
    today_start = datetime.combine(now.date(), datetime.min.time())
    today_end = datetime.combine(now.date(), datetime.max.time())

    cursor = connection.cursor(dictionary=True, buffered=True)
    assigned_name, staff_tag_id = get_assigned_name_and_tag(cursor, tag_id, connection) 
    
    if not assigned_name:
        logger.warning(f"Tag ID '{tag_id}' scanned but not assigned or not in staff list. Not processing.")
        cursor.close()
        return False

    if not check_cooldown(assigned_name):
        cursor.close()
        return False

    if not is_signin_allowed(current_time):
        logger.warning(f"Access denied for {assigned_name}. Sign-in/out not allowed at this time ({current_time.strftime('%H:%M')}).")
        cursor.close()
        return False

    # Update last tap time
    last_tap_times[assigned_name] = now

    # Determine current status from DB
    current_db_status_active, onsite_record_id = get_current_onsite_status(cursor, tag_id, today_start, today_end)
    
    db_action_successful = False
    sheet_update_status = "" # For daily sheet
    monthly_sheet_db_status = None # Status for monthly sheet logic (1 for IN, 0 for OUT)

    if current_db_status_active == 1: # User is currently IN, so this tap is a SIGN-OUT
        logger.info(f"User {assigned_name} (Tag: {tag_id}) currently IN. Processing SIGN-OUT.")
        db_action_successful = update_onsite_record_in_db(connection, cursor, onsite_record_id, now)
        sheet_update_status = "OUT"
        monthly_sheet_db_status = 0
    else: # User is currently OUT or no record for today, so this tap is a SIGN-IN
        logger.info(f"User {assigned_name} (Tag: {tag_id}) currently OUT. Processing SIGN-IN.")
        db_action_successful = add_onsite_record_to_db(connection, cursor, tag_id, now)
        sheet_update_status = "IN"
        monthly_sheet_db_status = 1
    
    cursor.close() # Close cursor after DB operations

    if db_action_successful:
        # Pass cached row index mappings to update functions
        update_monthly_sheet_status(month_sheet, assigned_name, staff_name_to_month_row_index, today_col_index, current_time, monthly_sheet_db_status)
        update_daily_sheet_row(daily_sheet, assigned_name, sheet_update_status, now.strftime("%H:%M:%S"), staff_name_to_daily_row_index)
        return True
    else:
        logger.error(f"Database operation failed for tag '{tag_id}'. Google Sheets not updated.")
        return False


def auto_mark_out_all_users(daily_sheet, connection, staff_name_to_daily_row_index):
    """Automatically marks all 'IN' users as 'OUT' after cutoff time in both DB and Daily Sheet."""
    current_time = datetime.now().time()
    if is_after_cutoff(current_time):
        logger.info("Cutoff time passed. Marking all remaining users as OUT...")
        
        cursor = connection.cursor(dictionary=True, buffered=True)
        today_start = datetime.combine(datetime.now().date(), datetime.min.time())
        today_end = datetime.combine(datetime.now().date(), datetime.max.time())

        # Get all users currently IN from DB
        _mysql_db_call(cursor.execute, """
            SELECT onsite_id, tag_id
            FROM onsite
            WHERE Active = 1 AND scan_date BETWEEN %s AND %s
        """, (today_start, today_end))
        users_to_mark_out = cursor.fetchall()

        daily_sheet_updates = []
        now_str = datetime.now().strftime("%H:%M:%S")

        if users_to_mark_out:
            logger.info(f"Found {len(users_to_mark_out)} users to auto-mark OUT in DB.")
            try:
                # Begin a transaction for batch DB updates
                connection.start_transaction()
                for user_record in users_to_mark_out:
                    onsite_id = user_record['onsite_id']
                    tag_id = user_record['tag_id']
                    
                    assigned_name = get_staff_name_by_tag_id(cursor, tag_id)
                    if not assigned_name:
                        logger.warning(f"Could not find staff name for tag_id '{tag_id}' during auto-mark out. Skipping DB update.")
                        continue

                    # Prepare DB update (don't commit yet)
                    _mysql_db_call(cursor.execute, """
                        UPDATE onsite
                        SET Active = %s, sign_out_date = %s
                        WHERE onsite_id = %s
                    """, (0, datetime.now(), onsite_id))
                    logger.info(f"DB: Prepared '{assigned_name}' (Tag: {tag_id}) for auto-mark as OUT.")

                    # Prepare Daily Sheet update (collect for batch update)
                    row = staff_name_to_daily_row_index.get(assigned_name)
                    if row:
                        daily_sheet_updates.append({"range": rowcol_to_a1(row, 3), "values": [["OUT"]]}) # Status
                        daily_sheet_updates.append({"range": rowcol_to_a1(row, 4), "values": [[now_str]]}) # Time
                    else:
                        logger.warning(f"Daily Sheet: '{assigned_name}' not found for auto-mark OUT update.")

                # Commit all DB updates at once
                connection.commit()
                logger.info(f"All auto-mark OUT DB updates committed.")

                # Perform batch update on Google Sheets
                if daily_sheet_updates:
                    _gsheet_api_call(daily_sheet.batch_update, daily_sheet_updates)
                    logger.info(f"Daily Sheet: Applied batch updates for auto-mark OUT.")
                else:
                    logger.info("No daily sheet updates to apply for auto-mark OUT.")

            except Error as e:
                logger.error(f"Error during auto-mark OUT batch process: {e}")
                connection.rollback() # Rollback all DB changes if any error occurs
            finally:
                cursor.close()
        else:
            logger.info("No users found in DB to auto-mark as OUT.")
            cursor.close() # Close cursor even if no users
    else:
        logger.info("Not past cutoff time. Auto-mark OUT skipped.")


# --- Main Execution ---

if __name__ == "__main__":
    if len(sys.argv) >= 2:
        example_tag_id = sys.argv[1].strip()
    else:
        example_tag_id = input("Enter the RFID Tag ID: ").strip()

    if not example_tag_id:
        logger.error("No tag ID provided. Exiting.")
        sys.exit(1)

    script_lock_fd = None
    connection = None
    try:
        logger.info(f"Attempting to acquire lock using file: {LOCK_FILE}")
        script_lock_fd = acquire_lock()

        if script_lock_fd is None:
            sys.exit(1)

        connection = get_db_connection()
        if not connection:
            logger.critical("Database connection failed. Exiting.")
            sys.exit(1)

        today = datetime.now().date()
        
        # Capture the cached data from initialize_sheets
        month_sheet, daily_sheet, today_col_index, staff_name_to_month_row_index, staff_name_to_daily_row_index = \
            initialize_sheets(workbook, connection, today)

        if example_tag_id.lower() == 'exit':
            logger.info("Exit command received, stopping.")
            sys.exit(0)

        logger.info(f"Processing RFID Tag: {example_tag_id}")
        success = on_tag_scan(example_tag_id, connection, month_sheet, daily_sheet, 
                              today_col_index, staff_name_to_month_row_index, staff_name_to_daily_row_index)

        if success:
            logger.info(f"Tag '{example_tag_id}' processed successfully.")
        else:
            logger.error(f"Failed to process tag '{example_tag_id}'.")

        # Pass the daily sheet name map for auto_mark_out_all_users
        auto_mark_out_all_users(daily_sheet, connection, staff_name_to_daily_row_index)

    except Error as e:
        logger.error(f"MySQL Database Error: {e}", exc_info=True)
    except gspread.exceptions.GSpreadException as e:
        logger.error(f"Google Sheets API Error: {e}", exc_info=True)
    except Exception as e:
        logger.critical(f"An unexpected error occurred: {e}", exc_info=True)
    finally:
        if connection and connection.is_connected():
            connection.close()
            logger.info("MySQL connection closed.")
        release_lock(script_lock_fd)
        logger.info("Script execution completed.")
        sys.exit(0)