import streamlit as st
from streamlit_autorefresh import st_autorefresh
from pymongo import MongoClient
import pandas as pd
import plotly.graph_objects as go

# =====================================================
# PAGE CONFIG
# =====================================================
st.set_page_config(
    page_title="IoT Big Data Dashboard",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =====================================================
# GLOBAL STYLE (ENTERPRISE UI)
# =====================================================
st.markdown("""
<style>
body {
    background: radial-gradient(circle at top, #0f172a, #020617);
    color: #e5e7eb;
}
.sidebar-title {
    font-size: 22px;
    font-weight: 800;
    margin-bottom: 20px;
}
.nav-card {
    padding: 14px 18px;
    border-radius: 14px;
    margin-bottom: 12px;
    background: linear-gradient(145deg, #111827, #020617);
    border: 1px solid #1f2937;
    cursor: pointer;
    transition: all .25s ease;
}
.nav-card:hover {
    background: linear-gradient(145deg, #1f2937, #020617);
    transform: translateX(4px);
}
.nav-active {
    background: linear-gradient(135deg, #2563eb, #1e40af);
    border: none;
}
.hero {
    padding: 40px;
    border-radius: 24px;
    background: linear-gradient(135deg, #0f172a, #020617);
    box-shadow: 0 30px 60px rgba(0,0,0,.6);
    margin-bottom: 30px;
}
.card {
    padding: 26px;
    border-radius: 20px;
    background: linear-gradient(145deg, #111827, #020617);
    box-shadow: 0 20px 40px rgba(0,0,0,.6);
}
.green { color: #22c55e; font-weight: 800; }
.yellow { color: #facc15; font-weight: 800; }
.red { color: #ef4444; font-weight: 800; }
</style>
""", unsafe_allow_html=True)

# =====================================================
# SESSION STATE (NAVIGATION)
# =====================================================
if "page" not in st.session_state:
    st.session_state.page = "overview"

def nav_button(label, page):
    active = st.session_state.page == page
    if st.sidebar.button(
        label,
        key=page,
        use_container_width=True
    ):
        st.session_state.page = page
    st.markdown(
        f"<div class='nav-card {'nav-active' if active else ''}'></div>",
        unsafe_allow_html=True
    )

# =====================================================
# SIDEBAR (CUSTOM)
# =====================================================
with st.sidebar:
    st.markdown("<div class='sidebar-title'>🌍 IoT Big Data</div>", unsafe_allow_html=True)

    if st.button("🏠 Overview", use_container_width=True):
        st.session_state.page = "overview"
    if st.button("📡 Realtime Monitoring", use_container_width=True):
        st.session_state.page = "realtime"
    if st.button("🧠 Analisis Kondisi", use_container_width=True):
        st.session_state.page = "analysis"
    if st.button("🔄 Big Data Pipeline", use_container_width=True):
        st.session_state.page = "pipeline"
    if st.button("🎯 Value & Insight", use_container_width=True):
        st.session_state.page = "value"
    if st.button("👥 About Us", use_container_width=True):
        st.session_state.page = "about"

# =====================================================
# DATABASE
# =====================================================
MONGO_URI = "mongodb+srv://irfan:irfan@cluster0.dsf740z.mongodb.net/?appName=Cluster0"
client = MongoClient(MONGO_URI)
db = client["iot_db"]
raw_col = db["dht22_logs"]
clean_col = db["dht22_clean"]



st_autorefresh(interval=3000, key="refresh")

# =====================================================
# HELPERS
# =====================================================

def get_latest_clean():
    data = list(clean_col.find().sort("window_end", -1).limit(1))
    return data[0] if data else None

def get_latest():
    d = list(raw_col.find().sort("created_at", -1).limit(1))
    return d[0] if d else None

def get_realtime(limit=40):
    d = list(raw_col.find().sort("created_at", -1).limit(limit))
    if not d:
        return pd.DataFrame()
    df = pd.DataFrame(d)
    df["created_at"] = pd.to_datetime(df["created_at"])
    return df.sort_values("created_at")

def analyze(temp, hum):
    if 22 <= temp <= 26 and 40 <= hum <= 70:
        return "Nyaman", "Rendah", "Tidak perlu tindakan", "green"
    elif temp > 26:
        return "Gerah", "Sedang", "Pertimbangkan pendinginan ruangan", "yellow"
    else:
        return "Tidak Stabil", "Sedang", "Pantau kondisi", "yellow"

# =====================================================
# PAGE: OVERVIEW
# =====================================================
if st.session_state.page == "overview":
    st.markdown("""
    <div class="hero">
        <h1>Implementasi Big Data Pipeline untuk Analisis dan Visualisasi Data Sensor Suhu Berbasis Internet of Things (IoT)</h1>
        <p>
        Proyek ini bertujuan untuk mengimplementasikan konsep Big Data Pipeline
        dalam pengolahan data sensor suhu berbasis Internet of Things (IoT).
        Data suhu diperoleh dari perangkat sensor IoT yang melakukan pencatatan
        suhu lingkungan secara berkala dan berkelanjutan.
        </p>
        <p>
        Data yang dihasilkan oleh sensor IoT bersifat time series dan terus
        bertambah seiring waktu. Oleh karena itu, diperlukan pendekatan Big Data
        untuk mengelola data tersebut secara sistematis, mulai dari proses
        pengumpulan data (ingestion), pembersihan dan transformasi data (ETL),
        penyimpanan data, hingga penyajian hasil dalam bentuk visualisasi.
        </p>
        <p>
        Website ini dikembangkan sebagai media visualisasi dan presentasi
        untuk menampilkan hasil implementasi Big Data Pipeline yang telah
        dirancang pada proyek ini.
        </p>
    </div>
    """, unsafe_allow_html=True)


# =====================================================
# PAGE: REALTIME
# =====================================================
elif st.session_state.page == "realtime":
    st.markdown("<div class='hero'><h2>📡 Realtime Monitoring</h2></div>", unsafe_allow_html=True)

    df = get_realtime()
    if df.empty:
        st.warning("Belum ada data.")
        st.stop()

    latest = df.iloc[-1]
    c1, c2 = st.columns(2)
    c1.metric("🌡 Temperature", f"{latest['temperature']:.2f} °C")
    c2.metric("💧 Humidity", f"{latest['humidity']:.2f} %")

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["created_at"], y=df["temperature"], name="Temperature"))
    fig.add_trace(go.Scatter(x=df["created_at"], y=df["humidity"], yaxis="y2", name="Humidity"))
    fig.update_layout(
        template="plotly_dark",
        yaxis2=dict(overlaying="y", side="right"),
        height=420
    )
    st.plotly_chart(fig, use_container_width=True)

# =====================================================
# PAGE: ANALYSIS
# =====================================================
elif st.session_state.page == "analysis":
    st.markdown("<div class='hero'><h2>🧠 Analisis Kondisi Ruangan</h2></div>", unsafe_allow_html=True)

    realtime = get_latest()
    clean = get_latest_clean()

    if not realtime or not clean:
        st.warning("Data belum lengkap.")
        st.stop()

    # REALTIME
    rt_temp = realtime["temperature"]
    rt_hum = realtime["humidity"]

    # CLEAN (ETL)
    avg_temp = clean["avg_temperature"]
    avg_hum = clean["avg_humidity"]
    condition = clean["condition"]
    risk = clean["risk_level"]
    rec = clean["recommendation"]

    c1, c2, c3 = st.columns(3)
    c1.metric("🌡 Realtime Temp", f"{rt_temp:.2f} °C")
    c2.metric("💧 Realtime Humidity", f"{rt_hum:.2f} %")
    c3.metric("🏷️ Kondisi (ETL)", condition)

    st.markdown(f"""
    <div class="card">
        <h3>Kondisi Berdasarkan Data Historis (ETL)</h3>
        <p><b>Avg Temperature:</b> {avg_temp:.2f} °C</p>
        <p><b>Avg Humidity:</b> {avg_hum:.2f} %</p>
        <p><b>Risiko:</b> {risk}</p>
        <p><b>Rekomendasi:</b> {rec}</p>
        <hr>
        <small>
        Analisis ini menggunakan data hasil ETL (clean data) sebagai dasar
        pengambilan keputusan, sementara nilai realtime digunakan
        untuk monitoring kondisi saat ini.
        </small>
    </div>
    """, unsafe_allow_html=True)


# =====================================================
# PAGE: PIPELINE
# =====================================================
elif st.session_state.page == "pipeline":
    st.markdown("<div class='hero'><h2>🔄 Big Data Pipeline</h2></div>", unsafe_allow_html=True)
    st.markdown("""
    <div class="card">
    <p>
    Big Data Pipeline pada proyek ini dirancang untuk mengelola data sensor suhu
    yang dihasilkan oleh perangkat IoT secara berkelanjutan. Pipeline ini
    memastikan data dapat diproses secara terstruktur dari tahap awal hingga
    tahap visualisasi.
    </p>

    <p><b>Alur Big Data Pipeline:</b></p>
    <ul>
        <li>Data suhu dikumpulkan dari sensor IoT sebagai data mentah (RAW).</li>
        <li>Data mentah disimpan ke dalam database MongoDB.</li>
        <li>Data kemudian diproses menggunakan PySpark melalui proses ETL.</li>
        <li>Hasil data bersih (CLEAN) disimpan kembali ke database.</li>
        <li>Data yang telah diolah divisualisasikan melalui dashboard.</li>
    </ul>

    <p>
    Pipeline ini mengacu pada konsep Big Data yang mencakup tahapan ingestion,
    pre-processing, storage, dan visualization.
    </p>
    </div>
    """, unsafe_allow_html=True)


# =====================================================
# PAGE: VALUE
# =====================================================
elif st.session_state.page == "value":
    st.markdown("<div class='hero'><h2>🎯 Value & Insight</h2></div>", unsafe_allow_html=True)
    st.markdown("""
    <div class="card">
    <p>
    Penerapan Big Data Pipeline pada proyek ini memberikan nilai tambah dalam
    pengelolaan data sensor suhu berbasis IoT. Data yang sebelumnya masih
    berupa data mentah dapat diolah menjadi data yang lebih terstruktur dan
    mudah dianalisis.
    </p>

    <ul>
        <li>Data suhu menjadi lebih rapi dan konsisten setelah melalui proses ETL.</li>
        <li>Visualisasi data membantu memahami pola perubahan suhu dari waktu ke waktu.</li>
        <li>Perbandingan suhu dapat dilakukan berdasarkan waktu dan lokasi.</li>
        <li>Pipeline Big Data mempermudah proses analisis data berskala besar.</li>
    </ul>

    <p>
    Nilai utama dari proyek ini terletak pada pemahaman penerapan konsep Big Data
    Pipeline dalam sistem berbasis IoT.
    </p>
    </div>
    """, unsafe_allow_html=True)


# =====================================================
# PAGE: ABOUT
# =====================================================
elif st.session_state.page == "about":
    st.markdown("<div class='hero'><h2>👥 About Us</h2></div>", unsafe_allow_html=True)
    st.markdown("""
    <div class="card">
    <b>Nama:</b> Muhammad Irfan Hakim<br>
    <b>Mata Kuliah:</b> Big Data<br>
    <b>Proyek:</b> IoT Environmental Monitoring
    </div>
    """, unsafe_allow_html=True)
