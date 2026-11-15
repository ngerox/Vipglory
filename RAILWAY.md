# Railway deploy (FF GLORY)

1. Push this folder to a **private** GitHub repo (it contains account/token files).
2. railway.com -> New Project -> Deploy from GitHub repo -> select it. Build/start are automatic (Procfile / railway.json).
3. Service -> Variables:
   - `ADMIN_USER` = NGANIL      `ADMIN_PASS` = (choose a strong password)
   - `DATA_DIR` = /data
   - (optional) `GOOGLE_CLIENT_ID`, `ADMIN_EMAILS` — or set the Client ID later in Admin Panel -> Google Login
4. Service -> Volumes -> Add Volume -> mount path `/data` (keeps users, coupons, settings after redeploy).
5. Settings -> Networking -> Generate Domain. Open `https://<domain>/` (users) and `https://<domain>/admin-x9k2` (admin).
6. Add `https://<domain>` to Google Console -> Authorized JavaScript origins.
