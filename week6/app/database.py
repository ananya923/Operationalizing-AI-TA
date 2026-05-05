"""
database.py — SQLite query layer for TechCorp knowledge base.

Handles all database lookups with field-level redaction based on user role.
Sensitive fields (salary, ssn, address) are only returned to authorized roles.
"""

import sqlite3
import json
import os
from typing import Optional

# Path to the database — resolved relative to this file's location
DB_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "data", "raw", "techcorp", "techcorp.db"
)

# Load access control config to know which roles can see which sensitive fields
_ACCESS_CONTROL_PATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "..",
    "data",
    "raw",
    "techcorp",
    "access_control.json",
)
with open(_ACCESS_CONTROL_PATH) as f:
    _ACCESS_CONTROL = json.load(f)

# Sensitive fields and which roles can see them
SENSITIVE_FIELDS = _ACCESS_CONTROL.get("sensitive_fields", {})

# Fields that are always redacted unless role is authorized
REDACTED_EMPLOYEE_FIELDS = {
    "ssn": ["hr", "finance"],
    "salary": ["executive", "hr", "finance"],
    "address": ["hr", "executive"],
}


def _get_conn() -> sqlite3.Connection:
    """Open a read-only connection to the TechCorp database."""
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _redact_employee(row: dict, user_role: str) -> dict:
    """Remove sensitive fields from an employee record if the role isn't authorized."""
    for field, allowed_roles in REDACTED_EMPLOYEE_FIELDS.items():
        if user_role not in allowed_roles:
            if field in row:
                row[field] = "[REDACTED]"
    return row


# ---------------------------------------------------------------------------
# Employee queries
# ---------------------------------------------------------------------------


def get_employee_by_name(name: str, user_role: str) -> Optional[dict]:
    """Look up an employee by (partial) name match."""
    with _get_conn() as conn:
        cur = conn.execute(
            "SELECT * FROM employees WHERE name LIKE ? LIMIT 1", (f"%{name}%",)
        )
        row = cur.fetchone()
        if row is None:
            return None
        return _redact_employee(dict(row), user_role)


def get_employee_by_id(employee_id: int, user_role: str) -> Optional[dict]:
    """Look up an employee by ID."""
    with _get_conn() as conn:
        cur = conn.execute("SELECT * FROM employees WHERE id = ?", (employee_id,))
        row = cur.fetchone()
        if row is None:
            return None
        return _redact_employee(dict(row), user_role)


def search_employees(
    department: Optional[str] = None,
    job_level: Optional[str] = None,
    user_role: str = "engineer",
    limit: int = 10,
) -> list[dict]:
    """Search employees by department and/or job level."""
    query = "SELECT * FROM employees WHERE 1=1"
    params = []
    if department:
        query += " AND department_name LIKE ?"
        params.append(f"%{department}%")
    if job_level:
        query += " AND job_level LIKE ?"
        params.append(f"%{job_level}%")
    query += f" LIMIT {limit}"

    with _get_conn() as conn:
        cur = conn.execute(query, params)
        rows = cur.fetchall()
        return [_redact_employee(dict(r), user_role) for r in rows]


def get_headcount_by_department(user_role: str) -> list[dict]:
    """Return employee count grouped by department."""
    with _get_conn() as conn:
        cur = conn.execute(
            "SELECT department_name, COUNT(*) as headcount FROM employees GROUP BY department_name ORDER BY headcount DESC"
        )
        return [dict(r) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Expense queries
# ---------------------------------------------------------------------------


def get_expenses_by_employee(employee_id: int, limit: int = 10) -> list[dict]:
    """Get recent expenses for a specific employee."""
    with _get_conn() as conn:
        cur = conn.execute(
            "SELECT * FROM expenses WHERE employee_id = ? ORDER BY date DESC LIMIT ?",
            (employee_id, limit),
        )
        return [dict(r) for r in cur.fetchall()]


def get_expense_summary_by_category(department: Optional[str] = None) -> list[dict]:
    """Summarize total expenses by category, optionally filtered by department."""
    if department:
        query = """
            SELECT e.category,
                   COUNT(*) as count,
                   ROUND(SUM(e.amount), 2) as total,
                   ROUND(AVG(e.amount), 2) as avg
            FROM expenses e
            JOIN employees emp ON e.employee_id = emp.id
            WHERE emp.department_name LIKE ?
            GROUP BY e.category
            ORDER BY total DESC
        """
        params = (f"%{department}%",)
    else:
        query = """
            SELECT category,
                   COUNT(*) as count,
                   ROUND(SUM(amount), 2) as total,
                   ROUND(AVG(amount), 2) as avg
            FROM expenses
            GROUP BY category
            ORDER BY total DESC
        """
        params = ()

    with _get_conn() as conn:
        cur = conn.execute(query, params)
        return [dict(r) for r in cur.fetchall()]


def get_total_expenses(year: Optional[int] = None) -> dict:
    """Get total expense amount, optionally filtered by year."""
    if year:
        query = "SELECT ROUND(SUM(amount), 2) as total, COUNT(*) as count FROM expenses WHERE strftime('%Y', date) = ?"
        params = (str(year),)
    else:
        query = "SELECT ROUND(SUM(amount), 2) as total, COUNT(*) as count FROM expenses"
        params = ()

    with _get_conn() as conn:
        cur = conn.execute(query, params)
        row = cur.fetchone()
        return dict(row) if row else {"total": 0, "count": 0}


# ---------------------------------------------------------------------------
# Project queries
# ---------------------------------------------------------------------------


def get_project_by_name(name: str) -> Optional[dict]:
    """Look up a project by (partial) name match."""
    with _get_conn() as conn:
        cur = conn.execute(
            "SELECT * FROM projects WHERE name LIKE ? LIMIT 1", (f"%{name}%",)
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_projects_by_status(status: str, limit: int = 10) -> list[dict]:
    """Get projects filtered by status (active, completed, on_hold, etc.)."""
    with _get_conn() as conn:
        cur = conn.execute(
            "SELECT * FROM projects WHERE status LIKE ? LIMIT ?", (f"%{status}%", limit)
        )
        return [dict(r) for r in cur.fetchall()]


def get_project_budget_summary() -> list[dict]:
    """Summarize budget vs spend across all projects."""
    with _get_conn() as conn:
        cur = conn.execute("""
            SELECT name, status, budget, spent,
                   ROUND(budget - spent, 2) as remaining,
                   ROUND(spent * 100.0 / budget, 1) as pct_used
            FROM projects
            ORDER BY budget DESC
            """)
        return [dict(r) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Benefits queries
# ---------------------------------------------------------------------------


def get_benefits_by_employee_id(employee_id: int) -> Optional[dict]:
    """Get benefits record for a specific employee."""
    with _get_conn() as conn:
        cur = conn.execute(
            "SELECT * FROM benefits WHERE employee_id = ?", (employee_id,)
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_pto_summary() -> dict:
    """Get company-wide PTO statistics."""
    with _get_conn() as conn:
        cur = conn.execute("""
            SELECT ROUND(AVG(pto_days), 1) as avg_pto_days,
                   ROUND(AVG(pto_used), 1) as avg_pto_used,
                   MIN(pto_days) as min_pto,
                   MAX(pto_days) as max_pto
            FROM benefits
            """)
        row = cur.fetchone()
        return dict(row) if row else {}


def get_health_plan_distribution() -> list[dict]:
    """Get distribution of health plan types across employees."""
    with _get_conn() as conn:
        cur = conn.execute("""
            SELECT health_plan, COUNT(*) as count
            FROM benefits
            GROUP BY health_plan
            ORDER BY count DESC
            """)
        return [dict(r) for r in cur.fetchall()]
