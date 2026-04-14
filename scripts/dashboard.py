import streamlit as st
import redis
import pandas as pd
import plotly.express as px
import time

# --- PAGE CONFIGURATION ---
# Setting up page titles and icons for a professional presentation look.
st.set_page_config(
    page_title="Hospital ER Dashboard",
    page_icon="🏥",
    layout="wide"
)

# --- CONFIGURATION & CONSTANTS ---
REDIS_HOST = "redis"
REDIS_PORT = 6379

# Target departments derived from the project syllabus (Section D).
TARGET_DEPARTMENTS = ["Orthopedics", "Cardiology", "Neurology", "Psychiatry"]

@st.cache_resource
def get_redis_connection():
    """Initializes and caches the Redis connection to optimize performance."""
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

def safe_int(value, default=0):
    """Utility to safely cast Redis strings to integers, preventing dashboard crashes."""
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default

def safe_float(value, default=0.0):
    """Utility to safely cast Redis strings to floats for average calculations."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return default

def load_dashboard_data(r):
    """
    Core Data Loading Logic:
    1. Fetches raw counters and hash maps from Redis.
    2. Calculates averages and formats data for visualization.
    """
    total_patients = safe_int(r.get("total_patients"))
    admitted_count = safe_int(r.get("admitted_count"))
    discharged_count = safe_int(r.get("discharged_count"))

    total_age = safe_float(r.get("total_age"))
    total_wait = safe_float(r.get("total_wait"))

    # Calculating live averages from cumulative totals stored in Redis.
    avg_age = (total_age / total_patients) if total_patients > 0 else 0.0
    avg_wait = (total_wait / total_patients) if total_patients > 0 else 0.0

    # Fetching department breakdown and filtering for target departments only.
    department_counts_raw = r.hgetall("department_counts")

    department_counts = {dept: 0 for dept in TARGET_DEPARTMENTS}
    for dept, count in department_counts_raw.items():
        if dept in TARGET_DEPARTMENTS:
            department_counts[dept] = safe_int(count)

    # Creating DataFrames for Plotly charts.
    dept_df = pd.DataFrame(
        {
            "Department": list(department_counts.keys()),
            "Patients": list(department_counts.values())
        }
    )

    status_df = pd.DataFrame(
        {
            "Status": ["Admitted", "Not Admitted / Discharged"],
            "Count": [admitted_count, discharged_count]
        }
    )

    return {
        "total_patients": total_patients,
        "admitted_count": admitted_count,
        "discharged_count": discharged_count,
        "avg_age": avg_age,
        "avg_wait": avg_wait,
        "dept_df": dept_df,
        "status_df": status_df
    }

def main():
    """
    Main UI Rendering:
    Organizes KPIs, Charts, and Tables into a clean, wide-screen layout.
    """
    st.title("🏥 Emergency Room Analytics Dashboard (Last Hour)")
    st.caption("Live view of incoming ER data based on the project pipeline")

    r = get_redis_connection()
    data = load_dashboard_data(r)

    # --- SECTION 1: KEY PERFORMANCE INDICATORS ---
    st.markdown("### Key Performance Indicators")
    col1, col2, col3, col4 = st.columns(4)

    col1.metric("Total Arrivals", data["total_patients"])
    col2.metric("Not Admitted / Discharged", data["discharged_count"])
    col3.metric("Average Wait Time", f"{data['avg_wait']:.2f} min")
    col4.metric("Average Age", f"{data['avg_age']:.2f} yrs")

    st.markdown("---")

    # --- SECTION 2: VISUAL ANALYTICS ---
    left_col, right_col = st.columns([2, 1])

    with left_col:
        st.subheader("Department Referrals")
        fig_dept = px.bar(
            data["dept_df"],
            x="Department",
            y="Patients",
            text="Patients",
            title="Patients Referred to Required Departments"
        )
        fig_dept.update_traces(textposition="outside")
        fig_dept.update_layout(
            xaxis_title="Department",
            yaxis_title="Number of Patients"
        )
        st.plotly_chart(fig_dept, width="stretch")

    with right_col:
        st.subheader("Admission Status")
        fig_status = px.pie(
            data["status_df"],
            names="Status",
            values="Count",
            title="Admitted vs Not Admitted"
        )
        st.plotly_chart(fig_status, width="stretch")

    # --- SECTION 3: DATA SUMMARY ---
    st.markdown("---")
    st.subheader("Summary Table")

    summary_df = pd.DataFrame(
        {
            "Metric": [
                "Total Arrivals",
                "Admitted",
                "Not Admitted / Discharged",
                "Average Wait Time (min)",
                "Average Age"
            ],
            "Value": [
                data["total_patients"],
                data["admitted_count"],
                data["discharged_count"],
                round(data["avg_wait"], 2),
                round(data["avg_age"], 2)
            ]
        }
    )

    st.dataframe(summary_df, width="stretch", hide_index=True)

    st.markdown("---")
    # Presentation tip for the final demo.
    st.info("Tip for presentation: explain that 'Not Admitted / Discharged' is derived from Patient Admission Flag = False.")

    st.markdown("---")
    # Presentation tip for the final demo.
    st.info("Tip for presentation: explain that 'Not Admitted / Discharged' is derived from Patient Admission Flag = False.")

    # auto-refresh the dashboard every 10 seconds to reflect new data from Redis.
    # wait 10 seconds before refreshing to allow new data to be in the dashboard 
    st.rerun()

if __name__ == "__main__":
    main()
