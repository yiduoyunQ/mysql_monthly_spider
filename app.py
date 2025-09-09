from flask import Flask, render_template, request
import pymysql
import platform
import os

# ----------------- 配置 -----------------
DB_HOST = "gateway01.ap-southeast-1.prod.aws.tidbcloud.com"
DB_PORT = 4000
DB_USER = "AYVUJoLj15SN2UA.root"
DB_PASS = "TtlRpMMyEf5Sl7rc"
DB_NAME = "dbmonthly"

PAGE_SIZE = 20

# ----------------- 系统证书 -----------------
def get_default_ca_path():
    system = platform.system()
    if system == "Darwin":
        return "/etc/ssl/cert.pem"
    elif system == "Linux":
        if os.path.exists("/etc/ssl/certs/ca-certificates.crt"):
            return "/etc/ssl/certs/ca-certificates.crt"
        elif os.path.exists("/etc/pki/tls/certs/ca-bundle.crt"):
            return "/etc/pki/tls/certs/ca-bundle.crt"
    elif system == "Windows":
        return None
    return None

def get_mysql_conn(ca_path):
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        charset="utf8mb4",
        ssl={"ca": ca_path} if ca_path else None
    )

# ----------------- Flask 应用 -----------------
app = Flask(__name__)
ca_path = get_default_ca_path()

def get_all_tags():
    """获取所有不重复标签"""
    conn = get_mysql_conn(ca_path)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT tag FROM t_articals")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return [r[0] for r in rows]

@app.route("/")
def index():
    page = int(request.args.get("page", 1))
    keyword = request.args.get("keyword", "").strip()
    selected_tag = request.args.get("tag", "").strip()
    offset = (page - 1) * PAGE_SIZE

    conn = get_mysql_conn(ca_path)
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    # 构建查询条件
    conditions = []
    params = []
    if keyword:
        conditions.append("(title LIKE %s OR tag LIKE %s)")
        like_kw = f"%{keyword}%"
        params.extend([like_kw, like_kw])
    if selected_tag:
        conditions.append("tag = %s")
        params.append(selected_tag)

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    # 查询总数
    cursor.execute(f"SELECT COUNT(*) AS total FROM t_articals {where_clause}", tuple(params))
    total = cursor.fetchone()["total"]

    # 查询分页文章
    cursor.execute(f"""
        SELECT * FROM t_articals
        {where_clause}
        ORDER BY create_date DESC
        LIMIT %s OFFSET %s
    """, tuple(params + [PAGE_SIZE, offset]))
    articles = cursor.fetchall()

    cursor.close()
    conn.close()

    total_pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
    all_tags = get_all_tags()

    return render_template("index.html",
                           articles=articles,
                           page=page,
                           total_pages=total_pages,
                           keyword=keyword,
                           selected_tag=selected_tag,
                           all_tags=all_tags)

# ----------------- 启动 Flask -----------------
if __name__ == "__main__":
    print("🚀 Flask app starting...")
    app.run(host="0.0.0.0", port=8000, debug=True, use_reloader=True)
