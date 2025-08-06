from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import os
import tempfile
import requests
import time
from datetime import datetime, timedelta
from google.oauth2 import service_account
import google.auth.transport.requests

app = Flask(__name__)
CORS(app)

### ---------- XLSX Call Duration Analysis ----------

def parse_duration(duration_str):
    try:
        h, m, s = map(int, str(duration_str).split(":"))
        return h * 3600 + m * 60 + s
    except:
        return 0

@app.route('/analyze-xlsx', methods=['POST'])
def analyze_xlsx():
    try:
        file = request.files['file']
        if not file:
            return jsonify({"error": "No file uploaded"}), 400

        df = pd.read_excel(file)

        required = ["Name", "Total Duration", "Missed Calls", "Voicemails"]
        for col in required:
            if col not in df.columns:
                return jsonify({"error": f"Missing column: {col}"}), 400

        df["Total Duration (secs)"] = df["Total Duration"].apply(parse_duration)
        df["Missed Calls"] = pd.to_numeric(df["Missed Calls"], errors='coerce').fillna(0)
        df["Voicemails"] = pd.to_numeric(df["Voicemails"], errors='coerce').fillna(0)
        df["Total Hours"] = (df["Total Duration (secs)"] / 3600).round(2)
        df["Total Missed or Voicemails"] = df["Missed Calls"] + df["Voicemails"]

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

        users = top_users_sorted[["Name", "Total Hours", "Missed Calls", "Voicemails"]].to_dict(orient='records')

        return jsonify({"users": users})

    except Exception as e:
        return jsonify({"error": "Internal Server Error", "details": str(e)}), 500

### ---------- Google Workspace Inactivity Analysis ----------

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
                    print(f"❗ Request failed ({response.status_code}): {url}")
                    return {}
            except Exception as e:
                print(f"❗ Exception fetching URL: {url} — {str(e)}")
                return {}

        print("📥 Fetching all user emails...")
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
        print(f"✅ Total users retrieved: {len(user_emails)}")

        print("\n📊 Analyzing login activity and storage usage...")
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
            try:
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
            except Exception as e:
                print(f"⚠️ Could not extract storage for {email}: {e}")

            inactive_users.append({
                "email": email,
                "last_login": last_login_dt.strftime("%Y-%m-%d"),
                "inactive_days": days_inactive,
                "storage_gb": round(total_storage_mb / 1024, 2)
            })

            time.sleep(0.5)

        inactive_users.sort(key=lambda x: x["storage_gb"], reverse=True)

        os.unlink(file_path)  # Clean up uploaded JSON
        return jsonify({"results": inactive_users})

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

### ---------- Run the App ----------

if __name__ == '__main__':
    app.run(port=5001)
