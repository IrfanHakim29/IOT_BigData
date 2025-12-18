import streamlit as st
from streamlit_autorefresh import st_autorefresh
from pymongo import MongoClient
import pandas as pd
import plotly.graph_objects as go
import time

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
# KONFIGURASI DATABASE & CACHING (PENTING!)
# =====================================================
# Ganti string ini dengan connection string Anda, tapi disarankan pakai st.secrets
# Format: "mongodb+srv://username:password@cluster..."
MONGO_URI = st.secrets["mongo"]["uri"]

@st.cache_resource
def init_connection():
    """
    Menginisialisasi koneksi ke MongoDB hanya sekali.
    Ini mencegah aplikasi membuat koneksi baru setiap kali refresh.
    """
    try:
        return MongoClient(MONGO_URI)
    except Exception as e:
        st.error(f"Gagal terhubung ke Database: {e}")
        return None

client = init_connection()

# Pastikan client berhasil terhubung sebelum lanjut
if client:
    db = client["iot_db"]
    raw_col = db["dht22_logs"]
    clean_col = db["dht22_clean"]
else:
    st.stop()

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
    margin-bottom: 20px;
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
    st.session_state.page = "about"

# =====================================================
# SIDEBAR
# =====================================================
with st.sidebar:
    st.markdown("<div class='sidebar-title'>🌍 IoT Big Data</div>", unsafe_allow_html=True)

    if st.button("📡 Realtime Monitoring", use_container_width=True):
        st.session_state.page = "realtime"
    if st.button("🧠 Analisis Kondisi", use_container_width=True):
        st.session_state.page = "analysis"
    if st.button("👥 About Us", use_container_width=True):
        st.session_state.page = "about"
    if st.button("📄 Data Table", use_container_width=True):
        st.session_state.page = "rawdata"
    if st.button("📊 Visualisasi ETL", use_container_width=True):
     st.session_state.page = "viz"


    st.markdown("---")
    # Auto refresh hanya aktif di halaman Realtime/Analisis untuk menghemat resource
    if st.session_state.page in ["realtime", "analysis", "viz"]:
        st_autorefresh(interval=3000, key="refresh") # 3 detik lebih stabil
        st.caption("🔄 Live updating...")

# =====================================================
# HELPERS (DATA FETCHING)
# =====================================================

def get_latest():
    """Mengambil 1 data paling baru dari RAW"""
    try:
        d = list(raw_col.find().sort("created_at", -1).limit(1))
        return d[0] if d else None
    except Exception:
        return None

def get_realtime(limit=100):
    """Mengambil N data terakhir untuk grafik"""
    try:
        d = list(raw_col.find().sort("created_at", -1).limit(limit))
        if not d:
            return pd.DataFrame()
        df = pd.DataFrame(d)
        if "created_at" in df.columns:
            df["created_at"] = pd.to_datetime(df["created_at"])
            return df.sort_values("created_at")
        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()

def get_rule_based_clean(rt_temp, rt_hum, temp_tolerance=1.0):
    """
    OPTIMIZED: Mengambil data ETL menggunakan query MongoDB ($gte, $lte)
    agar tidak perlu meload seluruh database ke memori.
    """
    try:
        # Query langsung ke database
        query = {
            "avg_temperature": {
                "$gte": rt_temp - temp_tolerance,
                "$lte": rt_temp + temp_tolerance
            }
        }
        
        # Ambil data yang cocok
        cursor = clean_col.find(query)
        data = list(cursor)

        if not data:
            return None

        df = pd.DataFrame(data)

        # Hitung selisih humidity di Python (karena aritmatika kompleks sulit di query standar Mongo)
        df["hum_diff"] = (df["avg_humidity"] - rt_hum).abs()
        
        # Ambil yang selisih humidity-nya paling kecil
        selected = df.sort_values("hum_diff").iloc[0]
        return selected.to_dict()
        
    except Exception as e:
        # st.error(f"Error fetching rules: {e}")
        return None

def get_raw_data(limit=500):
    """Mengambil data RAW untuk tabel"""
    try:
        d = list(raw_col.find().sort("created_at", -1).limit(limit))
        if not d:
            return pd.DataFrame()
        df = pd.DataFrame(d)
        if "_id" in df.columns:
            df = df.drop(columns=["_id"]) # Hapus _id agar tabel lebih rapi
        return df
    except Exception:
        return pd.DataFrame()
    
def get_clean_data(limit=500):
    """Ambil data ETL (clean)"""
    try:
        d = list(clean_col.find().sort("window_start", 1).limit(limit))
        if not d:
            return pd.DataFrame()
        df = pd.DataFrame(d)
        if "_id" in df.columns:
            df = df.drop(columns=["_id"])
        if "window_start" in df.columns:
            df["window_start"] = pd.to_datetime(df["window_start"])
        return df
    except Exception:
        return pd.DataFrame()
    

def get_clean_data_near_realtime(rt_temp, tolerance=1.0, limit=200):
    """
    Mengambil data ETL (clean) yang avg_temperature-nya
    mendekati suhu realtime (cluster berbasis suhu).
    """
    try:
        query = {
            "avg_temperature": {
                "$gte": rt_temp - tolerance,
                "$lte": rt_temp + tolerance
            }
        }

        d = list(
            clean_col
            .find(query)
            .sort("window_start", 1)
            .limit(limit)
        )

        if not d:
            return pd.DataFrame()

        df = pd.DataFrame(d)

        if "_id" in df.columns:
            df = df.drop(columns=["_id"])

        if "window_start" in df.columns:
            df["window_start"] = pd.to_datetime(df["window_start"])

        return df

    except Exception:
        return pd.DataFrame()



# =====================================================
# PAGE LOGIC
# =====================================================

# 1. ABOUT US
# =====================================================
# HALAMAN 5: ABOUT US (DIPERBARUI DENGAN TABS)
# =====================================================
if st.session_state.page == "about":
    st.markdown("<div class='hero'><h2>👥 About Us & Project</h2></div>", unsafe_allow_html=True)
    
    # Membuat 4 Tab agar halaman rapi
    tab_team, tab_topic, tab_pipe, tab_val = st.tabs(["👥 Tim & Anggota", "📚 Topik Proyek", "🔄 Big Data Pipeline", "🎯 Value & Insight"])

    # --- TAB 1: TIM & ANGGOTA ---
    with tab_team:
        col_a, col_b = st.columns([1, 2])
        with col_a:
            st.markdown("""
            <div class="card">
                <h3>👷 Kelompok 6</h3>
                <p><b>Mata Kuliah:</b> Big Data</p>
                <hr style="border-color: #334155;">
                <ul style="padding-left: 20px; margin: 0;">
                    <li>Muhammad Irfan Hakim</li>
                    <li>Henry Teja</li>
                    <li>Ridho Al Faiz</li>
                    <li>Muhammad Neshal Zuriel Aula</li>
                </ul>
            </div>""", unsafe_allow_html=True)
        
        with col_b:
            st.markdown("""
            <div class="card">
                <h3>📞 Kontak & Info</h3>
                <p><b>Universitas:</b> Politeknik Caltex Riau</p>
                <p><b>Mata Kuliah:</b> Big Data Engineering</p>
                <p><b>Tahun:</b> 2025-2026</p>
                <hr style="border-color: #334155;">
                <p><small>Tim ini berkomitmen pada pembelajaran praktis dan implementasi teknologi real-world dalam big data.</small></p>
            </div>""", unsafe_allow_html=True)

    # --- TAB 2: TOPIK PROYEK ---
    with tab_topic:
        st.markdown("## 📊 Implementasi Big Data Pipeline")
        st.markdown("""
Mengimplementasikan solusi **Big Data end-to-end** yang menggabungkan perangkat IoT fisik dengan pemrosesan data modern.
Sistem ini mengubah data sensor suhu mentah menjadi informasi yang bermakna melalui pipeline ETL terpadu dan visualisasi real-time.
        """)
        
        st.subheader("🎯 Tujuan Proyek")
        st.write("""
- Mengintegrasikan sensor IoT dengan infrastruktur cloud
- Membangun pipeline ETL yang robust dan scalable
- Menyediakan dashboard untuk monitoring dan analisis real-time
- Menghasilkan insights dari data historis untuk pengambilan keputusan
        """)
        
        st.subheader("📋 Ruang Lingkup")
        st.markdown("""
- **Input:** Data sensor suhu dan kelembapan dari perangkat IoT (ESP32 + DHT22)
- **Storage:** MongoDB cloud untuk penyimpanan data terstruktur
- **Processing:** ETL pipeline dengan Python dan Pandas untuk pembersihan dan agregasi data
- **Output:** Dashboard interaktif dengan visualisasi real-time dan analisis historis
        """)
        
        st.subheader("💡 Inovasi Proyek")
        st.write("""
Integrasi sempurna antara hardware IoT dengan cloud database dan dashboard analytics,
memberikan monitoring real-time dan insights berbasis data historis secara bersamaan.
        """)

    # --- TAB 3: BIG DATA PIPELINE ---
    with tab_pipe:
        st.subheader("🔄 Arsitektur Pipeline")
        st.write("""
Big Data Pipeline pada proyek ini dirancang untuk mengelola data sensor suhu
yang dihasilkan oleh perangkat IoT secara berkelanjutan. Pipeline ini
memastikan data dapat diproses secara terstruktur dari tahap awal hingga
tahap visualisasi.
        """)
        
        st.markdown("**Alur Big Data Pipeline:**")
        st.markdown("""
1. **Ingestion:** Data suhu dikumpulkan dari sensor IoT sebagai data mentah (RAW)
2. **Storage (Raw):** Data mentah disimpan ke dalam database MongoDB (Collection: `dht22_logs`)
3. **Processing (ETL):** Data diproses (Cleaning/Aggregation) menggunakan Python/PySpark
4. **Storage (Clean):** Hasil data bersih disimpan kembali ke database (Collection: `dht22_clean`)
5. **Visualization:** Data yang telah diolah divisualisasikan melalui dashboard Streamlit ini
        """)

    # --- TAB 4: VALUE & INSIGHT ---
    with tab_val:
        st.subheader("🎯 Value & Insight dari Big Data Pipeline")
        
        st.write("""
Implementasi Big Data Pipeline pada proyek ini memberikan nilai utama dalam
pengelolaan data sensor suhu berbasis Internet of Things (IoT) yang bersifat
time-series dan dihasilkan secara kontinu.
        """)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**💎 Nilai (Value) yang Dihasilkan**")
            st.markdown("""
- Data mentah sensor suhu (RAW) berhasil diolah menjadi data bersih (CLEAN)
- Kualitas data meningkat karena data null dan anomali difilter
- Data historis memungkinkan analisis tren suhu dari waktu ke waktu
- Dashboard menyediakan monitoring realtime dan analisis berbasis data
            """)
        
        with col2:
            st.markdown("**💡 Insight yang Diperoleh**")
            st.markdown("""
- Suhu ruangan menunjukkan pola fluktuasi yang konsisten berdasarkan waktu
- Rata-rata suhu dan kelembapan hasil ETL menjadi dasar evaluasi lingkungan
- Hasil analisis mendukung pengambilan keputusan (misal: kapan menyalakan AC)
- Real-time monitoring memungkinkan deteksi anomali secara cepat
            """)
        
        st.info("""
        Dengan pendekatan Big Data Pipeline, data sensor IoT tidak hanya ditampilkan
        secara realtime, tetapi juga diubah menjadi informasi yang memiliki makna
        untuk pengambilan keputusan strategis.
        """)
# 2. REALTIME MONITORING
elif st.session_state.page == "realtime":
    st.markdown("<div class='hero'><h2>📡 Realtime Monitoring</h2></div>", unsafe_allow_html=True)

    df = get_realtime()
    
    # Indikator Live
    latest = get_latest()
    
    if latest:
        # Metrics Row
        m1, m2, m3 = st.columns(3)
        m1.metric("🌡 Temperature", f"{latest.get('temperature', 0):.2f} °C")
        m2.metric("💧 Humidity", f"{latest.get('humidity', 0):.2f} %")
        m3.caption(f"Last Update: {latest.get('created_at', 'N/A')}")
    else:
        st.warning("Menunggu data masuk dari sensor...")

    # Chart
    if not df.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df["created_at"], y=df["temperature"], 
            mode='lines', name="Temperature",
            line=dict(color="#f43f5e", width=3)
        ))
        fig.add_trace(go.Scatter(
            x=df["created_at"], y=df["humidity"], 
            yaxis="y2", mode='lines', name="Humidity",
            line=dict(color="#3b82f6", width=3)
        ))
        
        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            yaxis2=dict(overlaying="y", side="right", showgrid=False),
            hovermode="x unified",
            height=450,
            legend=dict(orientation="h", y=1.1)
        )
        st.plotly_chart(fig, use_container_width=True)

# 3. ANALISIS KONDISI
elif st.session_state.page == "analysis":
    st.markdown("<div class='hero'><h2>🧠 Analisis Cerdas (Rule Based)</h2></div>", unsafe_allow_html=True)

    realtime = get_latest()
    
    if not realtime:
        st.error("Tidak ada data sensor aktif. Nyalakan alat IoT Anda.")
        st.stop()

    rt_temp = realtime.get("temperature", 0)
    rt_hum = realtime.get("humidity", 0)

    # Cari data pembanding dari database Clean (ETL)
    clean = get_rule_based_clean(rt_temp, rt_hum)

    # Tampilan Layout
    col_real, col_result = st.columns([1, 2])

    with col_real:
        st.markdown(f"""
        <div class="card">
            <h3>📡 Input Realtime</h3>
            <h1 style="color:#3b82f6">{rt_temp:.1f} °C</h1>
            <h3 style="color:#94a3b8">{rt_hum:.1f} %</h3>
            <small>Data diterima dari sensor</small>
        </div>
        """, unsafe_allow_html=True)

    with col_result:
        if clean:
            condition = clean.get("condition", "Unknown")
            risk = clean.get("risk_level", "Unknown")
            rec = clean.get("recommendation", "-")
            
            # Tentukan warna card
            card_color = "#22c55e" # Green
            if condition == "Gerah": card_color = "#facc15" 
            elif condition == "Panas" or "Bahaya" in risk: card_color = "#ef4444"

            st.markdown(f"""
            <div class="card" style="border-left: 5px solid {card_color};">
                <h3>🔍 Hasil Analisis</h3>
                <p>Berdasarkan kemiripan dengan pola data historis (ETL):</p>
                <h2 style="color:{card_color}">{condition}</h2>
                <p><b>Tingkat Risiko:</b> {risk}</p>
                <p><b>Rekomendasi Sistem:</b><br> {rec}</p>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.info("Sedang mempelajari pola... Data historis yang mirip belum ditemukan.")

# 4. RAW DATA
elif st.session_state.page == "rawdata":
    st.markdown("<div class='hero'><h2>📄 Data Mentah (MongoDB)</h2></div>", unsafe_allow_html=True)
    
    df = get_raw_data()
    if not df.empty:
        st.dataframe(df, use_container_width=True, height=600)
    else:
        st.warning("Database kosong.")

# 5. VISUALISASI ETL
elif st.session_state.page == "viz":
    st.markdown("<div class='hero'><h2>📊 Visualisasi ETL (Berbasis Realtime)</h2></div>", unsafe_allow_html=True)

    # 1️⃣ Ambil data realtime dulu
    realtime = get_latest()
    if not realtime:
        st.warning("Data realtime belum tersedia.")
        st.stop()

    rt_temp = realtime.get("temperature", 0)

  
    

    # 3️⃣ Ambil data ETL yang mendekati realtime
    df = get_clean_data_near_realtime(rt_temp)
    if df.empty:
        st.warning("Tidak ditemukan data ETL dengan suhu mendekati realtime.")
        st.stop()

    # 4️⃣ BARU tampilkan metric
    c1, c2, c3 = st.columns(3)
    c1.metric("🌡 Realtime Suhu", f"{rt_temp:.2f} °C")
    c2.metric("📊 Avg Suhu Cluster ETL", f"{df['avg_temperature'].mean():.2f} °C")
    c3.metric("📦 Jumlah Window Mirip", len(df))



    # =========================
    # LINE CHART AVG TEMP & HUM
    # =========================
    fig_line = go.Figure()

    fig_line.add_trace(go.Scatter(
        x=df["window_start"],
        y=df["avg_temperature"],
        mode="lines",
        name="Avg Temperature (°C)",
        line=dict(color="#ef4444", width=3)
    ))

    fig_line.add_trace(go.Scatter(
        x=df["window_start"],
        y=df["avg_humidity"],
        mode="lines",
        name="Avg Humidity (%)",
        yaxis="y2",
        line=dict(color="#3b82f6", width=3)
    ))

    fig_line.update_layout(
        template="plotly_dark",
        height=420,
        hovermode="x unified",
        yaxis2=dict(overlaying="y", side="right"),
        legend=dict(orientation="h", y=1.1)
    )

    st.plotly_chart(fig_line, use_container_width=True)

    # =========================
    # SCATTER TEMP vs HUM
    # =========================
    fig_scatter = go.Figure()

    fig_scatter.add_trace(go.Scatter(
        x=df["avg_temperature"],
        y=df["avg_humidity"],
        mode="markers",
        marker=dict(size=8, color=df["avg_temperature"], colorscale="Turbo"),
        name="Temp vs Hum"
    ))

    fig_scatter.update_layout(
        template="plotly_dark",
        title="Hubungan Suhu dan Kelembapan",
        xaxis_title="Rata-rata Suhu (°C)",
        yaxis_title="Rata-rata Kelembapan (%)",
        height=400
    )

    st.plotly_chart(fig_scatter, use_container_width=True)

    # =========================
    # HEATMAP KORELASI
    # =========================
    corr = df[["avg_temperature", "avg_humidity"]].corr()

    fig_corr = go.Figure(
        data=go.Heatmap(
            z=corr.values,
            x=corr.columns,
            y=corr.columns,
            colorscale="RdBu",
            zmin=-1, zmax=1
        )
    )

    fig_corr.update_layout(
        template="plotly_dark",
        title="Korelasi Suhu dan Kelembapan",
        height=350
    )

    st.plotly_chart(fig_corr, use_container_width=True)

    fig_line.add_hline(
        y=rt_temp,
        line_dash="dash",
        line_color="white",
        annotation_text="Realtime Temp",
        annotation_position="top left"
    )
