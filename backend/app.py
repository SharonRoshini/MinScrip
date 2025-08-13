from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
import json
import time
from datetime import datetime, date, timedelta
from collections import defaultdict
import base64
from typing import Dict, List, Any
import tempfile
import pandas as pd
import google.auth.transport.requests
from google.oauth2 import service_account

app = Flask(__name__)
CORS(app)

# ------------------ Utility ------------------

def parse_duration(duration_str):
    try:
        h, m, s = map(int, str(duration_str).split(":"))
        return h * 3600 + m * 60 + s
    except:
        return 0

# ------------------ XLSX Call Duration Analysis ------------------

@app.route('/analyze-xlsx', methods=['POST'])
def analyze_xlsx():
    try:
        file = request.files['file']
        if not file:
            return jsonify({"error": "No file uploaded"}), 400

        df = pd.read_excel(file)

        rename_map = {
            "Inbound total no.of Calls": "Inbound Calls",
            "Outbound total no.of Calls": "Outbound Calls"
        }
        df.rename(columns=rename_map, inplace=True)

        required = [
            "Name", "Total Duration", "Missed Calls", "Voicemails",
            "Inbound Calls", "Outbound Calls", "Inbound Duration", "Outbound Duration"
        ]
        for col in required:
            if col not in df.columns:
                return jsonify({"error": f"Missing column: {col}"}), 400

        df["Total Duration (secs)"] = df["Total Duration"].apply(parse_duration)
        df["Inbound Duration (secs)"] = df["Inbound Duration"].apply(parse_duration)
        df["Outbound Duration (secs)"] = df["Outbound Duration"].apply(parse_duration)

        df["Missed Calls"] = pd.to_numeric(df["Missed Calls"], errors='coerce').fillna(0)
        df["Voicemails"] = pd.to_numeric(df["Voicemails"], errors='coerce').fillna(0)
        df["Inbound Calls"] = pd.to_numeric(df["Inbound Calls"], errors='coerce').fillna(0)
        df["Outbound Calls"] = pd.to_numeric(df["Outbound Calls"], errors='coerce').fillna(0)

        df["Total Hours"] = (df["Total Duration (secs)"] / 3600).round(2)
        df["Total Missed or Voicemails"] = df["Missed Calls"] + df["Voicemails"]

        df["Call Ratio (Inbound/Outbound)"] = df.apply(
            lambda row: round(row["Inbound Calls"] / row["Outbound Calls"], 2)
            if row["Outbound Calls"] != 0 else "Inf", axis=1
        )

        df["Duration Ratio (Inbound/Outbound)"] = df.apply(
            lambda row: round(row["Inbound Duration (secs)"] / row["Outbound Duration (secs)"], 2)
            if row["Outbound Duration (secs)"] != 0 else "Inf", axis=1
        )

        high_hours_threshold = df["Total Hours"].quantile(0.75)
        high_missed_vm_threshold = df["Total Missed or Voicemails"].quantile(0.75)

        top_users = df[
            (df["Total Hours"] >= high_hours_threshold) |
            (df["Total Missed or Voicemails"] >= high_missed_vm_threshold)
        ]

        top_users_sorted = top_users.sort_values(
            by=["Total Hours", "Total Missed or Voicemails"],
            ascending=[False, False]
        )

        users = top_users_sorted[[
            "Name", "Total Hours", "Missed Calls", "Voicemails",
            "Call Ratio (Inbound/Outbound)", "Duration Ratio (Inbound/Outbound)"
        ]].to_dict(orient='records')

        return jsonify({"users": users})

    except Exception as e:
        return jsonify({"error": "Internal Server Error", "details": str(e)}), 500

# ------------------ Google Workspace Inactivity Analysis ------------------

@app.route('/upload', methods=['POST'])
def upload_service_account():
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file uploaded'}), 400

        file = request.files['file']
        inactivity_days = int(request.form.get('inactivity_days'))
        admin_email = request.form.get('admin_email')

        if not admin_email:
            return jsonify({'error': 'Admin email is required'}), 400

        temp = tempfile.NamedTemporaryFile(delete=False)
        file_path = temp.name
        temp.close()
        file.save(file_path)

        SCOPES = [
            'https://www.googleapis.com/auth/admin.directory.user.readonly',
            'https://www.googleapis.com/auth/admin.reports.usage.readonly',
            'https://www.googleapis.com/auth/admin.reports.audit.readonly'
        ]

        credentials = service_account.Credentials.from_service_account_file(
            file_path,
            scopes=SCOPES,
            subject=admin_email
        )
        request_adapter = google.auth.transport.requests.Request()
        credentials.refresh(request_adapter)
        token = credentials.token
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }

        def safe_get_json(url):
            try:
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    return response.json()
                else:
                    return {}
            except Exception:
                return {}

        user_emails = []
        page_token = None
        while True:
            url = 'https://admin.googleapis.com/admin/directory/v1/users?customer=my_customer&maxResults=500'
            if page_token:
                url += f"&pageToken={page_token}"
            res = safe_get_json(url)
            user_emails.extend([u['primaryEmail'] for u in res.get('users', [])])
            page_token = res.get('nextPageToken')
            if not page_token:
                break

        now = datetime.utcnow()
        inactive_users = []

        for email in user_emails:
            login_url = f"https://admin.googleapis.com/admin/reports/v1/activity/users/{email}/applications/login?maxResults=1"
            login_data = safe_get_json(login_url)

            events = login_data.get("items", [])
            if not events or not events[0].get("id", {}).get("time"):
                continue

            last_login_str = events[0]["id"]["time"]
            try:
                last_login_dt = datetime.strptime(last_login_str, "%Y-%m-%dT%H:%M:%S.%fZ")
            except ValueError:
                last_login_dt = datetime.strptime(last_login_str, "%Y-%m-%dT%H:%M:%SZ")

            days_inactive = (now - last_login_dt).days
            if days_inactive < inactivity_days:
                continue

            total_storage_mb = 0
            for days_ago in range(4, 30):
                usage_date = (now - timedelta(days=days_ago)).strftime("%Y-%m-%d")
                usage_url = f"https://admin.googleapis.com/admin/reports/v1/usage/users/{email}/dates/{usage_date}"
                usage_data = safe_get_json(usage_url)
                reports = usage_data.get("usageReports", [])
                if not reports:
                    continue
                parameters = reports[0].get("parameters", [])
                for param in parameters:
                    if param.get("name") == "accounts:used_quota_in_mb":
                        total_storage_mb = float(param.get("intValue", 0))
                        break
                if total_storage_mb > 0:
                    break

            inactive_users.append({
                "email": email,
                "last_login": last_login_dt.strftime("%Y-%m-%d"),
                "inactive_days": days_inactive,
                "storage_gb": round(total_storage_mb / 1024, 2)
            })

            time.sleep(0.5)

        inactive_users.sort(key=lambda x: x["storage_gb"], reverse=True)

        return jsonify({"results": inactive_users})

    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ------------------ JIRA Productivity Analyzer ------------------

class JiraProductivityAnalyzer:
    def __init__(self, base_url: str, username: str, api_token: str):
        self.base_url = base_url.rstrip('/')
        self.auth = base64.b64encode(f"{username}:{api_token}".encode()).decode()
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Basic {self.auth}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })

    def fetch_all_issues(self, project_key: str = None, batch_size: int = 100) -> Dict[str, Any]:
        url = f"{self.base_url}/rest/api/3/search"
        all_issues = []
        start_at = 0
        total_issues = 0
        fetched_count = 0

        jql_query = f'project = "{project_key}" ORDER BY created DESC' if project_key and project_key.upper() != 'ALL' else 'ORDER BY created DESC'

        request_template = {
            "jql": jql_query,
            "expand": ["names", "schema"],
            "fields": [
                "summary", "status", "assignee", "created", "resolutiondate",
                "duedate", "priority", "issuetype", "timespent",
                "timeoriginalestimate", "worklog", "project"
            ]
        }

        try:
            while True:
                request_body = {
                    **request_template,
                    "startAt": start_at,
                    "maxResults": batch_size
                }
                response = self.session.post(url, json=request_body)
                response.raise_for_status()
                data = response.json()

                if start_at == 0:
                    total_issues = data['total']

                all_issues.extend(data['issues'])
                fetched_count += len(data['issues'])
                start_at += batch_size

                if fetched_count >= total_issues or len(data['issues']) == 0:
                    break
                time.sleep(0.2)

            return {
                'issues': all_issues,
                'total': total_issues,
                'startAt': 0,
                'maxResults': len(all_issues)
            }
        except requests.exceptions.RequestException as e:
            print(f"Error fetching JIRA issues: {e}")
            raise

    def analyze_productivity(self, jira_data: Dict[str, Any]) -> Dict[str, Any]:
        issues = []
        for issue in jira_data['issues']:
            fields = issue['fields']
            created_date = datetime.fromisoformat(fields['created'].replace('Z', '+00:00')).date() if fields.get('created') else None
            resolution_date = datetime.fromisoformat(fields['resolutiondate'].replace('Z', '+00:00')).date() if fields.get('resolutiondate') else None

            issue_data = {
                'key': issue['key'],
                'summary': fields.get('summary', ''),
                'status': fields['status']['name'],
                'status_category': fields['status']['statusCategory']['name'],
                'assignee': fields['assignee']['displayName'] if fields.get('assignee') else 'Unassigned',
                'assignee_id': fields['assignee']['accountId'] if fields.get('assignee') else None,
                'created': created_date,
                'resolution_date': resolution_date,
                'due_date': datetime.fromisoformat(fields['duedate']).date() if fields.get('duedate') else None,
                'priority': fields['priority']['name'] if fields.get('priority') else 'None',
                'issue_type': fields['issuetype']['name'],
                'time_spent': fields.get('timespent', 0) or 0,
                'original_estimate': fields.get('timeoriginalestimate', 0) or 0
            }
            issues.append(issue_data)

        user_stats = self.calculate_user_stats(issues)
        overall_stats = self.calculate_overall_stats(issues)
        return {
            'issues': issues,
            'user_stats': user_stats,
            'overall_stats': overall_stats,
            'total_issues': jira_data['total']
        }

    def calculate_user_stats(self, issues: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        stats = defaultdict(lambda: {
            'assignee': '', 'total': 0, 'completed': 0, 'in_progress': 0, 'todo': 0,
            'completion_rate': 0, 'total_time_spent': 0, 'avg_time_per_task': 0,
            'completed_this_month': 0, 'overdue_tasks': 0
        })
        current_month = date.today().month
        current_year = date.today().year
        today = date.today()
        for issue in issues:
            assignee = issue['assignee']
            if assignee == 'Unassigned':
                continue
            stats[assignee]['assignee'] = assignee
            stats[assignee]['total'] += 1
            stats[assignee]['total_time_spent'] += issue['time_spent']
            status_category = issue['status_category'].lower()
            if status_category == 'done':
                stats[assignee]['completed'] += 1
                if issue['resolution_date'] and issue['resolution_date'].month == current_month and issue['resolution_date'].year == current_year:
                    stats[assignee]['completed_this_month'] += 1
            elif 'progress' in status_category:
                stats[assignee]['in_progress'] += 1
            else:
                stats[assignee]['todo'] += 1
            if issue['due_date'] and issue['due_date'] < today and status_category != 'done':
                stats[assignee]['overdue_tasks'] += 1
        for user_stat in stats.values():
            if user_stat['total'] > 0:
                user_stat['completion_rate'] = round((user_stat['completed'] / user_stat['total']) * 100, 1)
            if user_stat['completed'] > 0:
                user_stat['avg_time_per_task'] = round(user_stat['total_time_spent'] / user_stat['completed'])
        return dict(stats)

    def calculate_overall_stats(self, issues: List[Dict[str, Any]]) -> Dict[str, Any]:
        total = len(issues)
        completed = sum(1 for i in issues if i['status_category'].lower() == 'done')
        in_progress = sum(1 for i in issues if 'progress' in i['status_category'].lower())
        todo = total - completed - in_progress
        today = date.today()
        overdue = sum(1 for i in issues if i['due_date'] and i['due_date'] < today and i['status_category'].lower() != 'done')
        return {
            'total': total, 'completed': completed, 'in_progress': in_progress,
            'todo': todo, 'overdue': overdue,
            'completion_rate': round((completed / total) * 100, 1) if total > 0 else 0
        }

# ------------------ JIRA API Endpoint ------------------

@app.route('/analyze-jira', methods=['POST'])
def analyze_jira():
    try:
        data = request.json
        base_url = data.get('base_url')
        username = data.get('username')
        api_token = data.get('api_token')
        project_key = data.get('project_key', '')

        if not all([base_url, username, api_token]):
            return jsonify({"error": "Missing required parameters."}), 400

        analyzer = JiraProductivityAnalyzer(base_url, username, api_token)
        result = analyzer.fetch_all_issues(project_key)
        analysis = analyzer.analyze_productivity(result)
        report = analyzer.calculate_overall_stats(analysis['issues'])

        return jsonify({
            "summary": report,
            "user_stats": analysis['user_stats']
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ------------------ Flask App Entry ------------------

if __name__ == '__main__':
    app.run(debug=True, port=5002)
