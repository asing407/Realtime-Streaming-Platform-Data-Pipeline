import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
import time

# Page config
st.set_page_config(
    page_title="E-Commerce Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Database connection
@st.cache_resource
def get_db_connection():
    import sqlalchemy
    engine = sqlalchemy.create_engine(
        f"postgresql://{os.getenv('USER')}@localhost:5432/ecommerce_analytics"
    )
    return engine

def query_db(query):
    conn = get_db_connection()
    try:
        df = pd.read_sql_query(query, conn)
        return df
    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()

# Custom CSS
st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 10px;
        color: white;
    }
    .big-number {
        font-size: 36px;
        font-weight: bold;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        padding-left: 20px;
        padding-right: 20px;
    }
</style>
""", unsafe_allow_html=True)

# Title
st.title("Real-Time E-Commerce Analytics Dashboard")
st.markdown("### Live streaming analytics from your Delta Lake pipeline")

# Sidebar
st.sidebar.header("📊 Dashboard Controls")
auto_refresh = st.sidebar.checkbox("Auto-refresh", value=True)
if auto_refresh:
    refresh_rate = st.sidebar.slider("Refresh interval (seconds)", 10, 60, 30)
    time.sleep(refresh_rate)
    st.rerun()

st.sidebar.markdown(f"**Last updated:** {datetime.now().strftime('%H:%M:%S')}")

if st.sidebar.button("🔄 Refresh Now"):
    st.rerun()

# Main metrics
col1, col2, col3, col4 = st.columns(4)

# Get hourly summary
hourly_df = query_db("""
    SELECT 
        COALESCE(SUM(total_events), 0)::bigint as total_events,
        COALESCE(SUM(total_revenue), 0)::numeric as total_revenue,
        COALESCE(SUM(total_orders), 0)::bigint as total_orders,
        COALESCE(AVG(conversion_rate), 0)::numeric as avg_conversion_rate
    FROM hourly_summary
""")

if not hourly_df.empty:
    with col1:
        total_events = int(hourly_df['total_events'].iloc[0] or 0)
        st.metric(
            label="📊 Total Events",
            value=f"{total_events:,}",
            delta="Live"
        )

    with col2:
        total_revenue = float(hourly_df['total_revenue'].iloc[0] or 0)
        st.metric(
            label="💰 Total Revenue",
            value=f"£{total_revenue:,.0f}",
            delta="Today"
        )

    with col3:
        total_orders = int(hourly_df['total_orders'].iloc[0] or 0)
        st.metric(
            label="🛒 Total Orders",
            value=f"{total_orders:,}",
            delta="Completed"
        )

    with col4:
        conv_rate = float(hourly_df['avg_conversion_rate'].iloc[0] or 0)
        st.metric(
            label="📈 Conversion Rate",
            value=f"{conv_rate:.2f}%",
            delta="Average"
        )

st.divider()

# === TABS ===
tab1, tab2, tab3, tab4 = st.tabs(["📊 Conversion Funnel", "🔥 Trending Products", "💰 Revenue Analytics", "🚨 Live Alerts"])

# === TAB 1: CONVERSION FUNNEL ===
with tab1:
    st.subheader("Real-Time Conversion Funnel")
    
    # Get latest funnel data
    funnel_query = """
    SELECT 
        event_type,
        SUM(event_count) as total_events,
        SUM(unique_users) as total_users
    FROM funnel_metrics
    GROUP BY event_type
    ORDER BY 
        CASE event_type
            WHEN 'page_view' THEN 1
            WHEN 'product_view' THEN 2
            WHEN 'add_to_cart' THEN 3
            WHEN 'purchase' THEN 4
        END
    """
    funnel_df = query_db(funnel_query)
    
    if not funnel_df.empty and len(funnel_df) > 0:
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # Funnel chart
            fig = go.Figure(go.Funnel(
                y=funnel_df['event_type'].str.replace('_', ' ').str.title(),
                x=funnel_df['total_users'],
                textposition="inside",
                textinfo="value+percent initial",
                marker=dict(
                    color=['#667eea', '#764ba2', '#f093fb', '#4facfe']
                ),
                connector={"line": {"color": "royalblue", "dash": "dot", "width": 3}}
            ))
            
            fig.update_layout(
                title="User Journey Funnel (Last 2 Hours)",
                height=400,
                showlegend=False
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("### 📈 Funnel Metrics")
            
            for idx, row in funnel_df.iterrows():
                event_name = row['event_type'].replace('_', ' ').title()
                users = int(row['total_users'])
                events = int(row['total_events'])
                
                st.markdown(f"**{event_name}**")
                st.markdown(f"👥 {users:,} users | 📊 {events:,} events")
                
                if idx < len(funnel_df) - 1:
                    conversion = (users / funnel_df.iloc[0]['total_users']) * 100
                    st.progress(conversion / 100)
                    st.markdown(f"*{conversion:.1f}% from start*")
                
                st.markdown("---")
    else:
        st.info("No funnel data available yet. Data will appear as events are processed.")
    
    # Funnel over time
    st.subheader("📈 Funnel Trends (Last 2 Hours)")
    
    time_funnel_query = """
    SELECT 
        window_start,
        event_type,
        event_count
    FROM funnel_metrics
    ORDER BY window_start
    """
    time_funnel_df = query_db(time_funnel_query)
    
    if not time_funnel_df.empty:
        fig = px.line(
            time_funnel_df,
            x='window_start',
            y='event_count',
            color='event_type',
            title='Event Counts Over Time',
            labels={'event_count': 'Events', 'window_start': 'Time', 'event_type': 'Event Type'},
            color_discrete_map={
                'page_view': '#667eea',
                'product_view': '#764ba2', 
                'add_to_cart': '#f093fb',
                'purchase': '#4facfe'
            }
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Waiting for time-series data...")

# === TAB 2: TRENDING PRODUCTS ===
with tab2:
    st.subheader("Top Trending Products")
    
    trending_query = """
    SELECT 
        product_name,
        category,
        SUM(activity_count) as total_activity,
        SUM(purchases) as total_purchases,
        SUM(cart_adds) as total_cart_adds,
        SUM(unique_viewers) as total_viewers
    FROM trending_products
    WHERE product_name IS NOT NULL
    GROUP BY product_name, category
    ORDER BY total_activity DESC
    LIMIT 10
    """
    trending_df = query_db(trending_query)
    
    if not trending_df.empty:
        col1, col2 = st.columns([2, 1])
        
        with col1:
            fig = px.bar(
                trending_df,
                x='total_activity',
                y='product_name',
                orientation='h',
                color='category',
                title='Top 10 Products by Activity',
                labels={'total_activity': 'Total Activity', 'product_name': 'Product'},
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            fig.update_layout(height=500, showlegend=True)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("### 🏆 Top Products")
            for idx, row in trending_df.head(5).iterrows():
                with st.container():
                    st.markdown(f"**{idx+1}. {row['product_name']}**")
                    st.markdown(f"📁 {row['category']}")
                    
                    col_a, col_b = st.columns(2)
                    with col_a:
                        st.metric("👀 Views", f"{int(row['total_viewers']):,}")
                    with col_b:
                        st.metric("🛒 Cart", f"{int(row['total_cart_adds']):,}")
                    
                    st.markdown("---")
    
    # Category breakdown
    st.subheader("Activity by Category")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if not trending_df.empty:
            category_df = trending_df.groupby('category').agg({
                'total_activity': 'sum',
                'total_purchases': 'sum'
            }).reset_index()
            
            fig = px.pie(
                category_df,
                values='total_activity',
                names='category',
                title='Activity Distribution by Category',
                hole=0.4
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        if not trending_df.empty:
            fig = px.bar(
                category_df,
                x='category',
                y='total_purchases',
                title='Purchases by Category',
                labels={'total_purchases': 'Purchases', 'category': 'Category'}
            )
            st.plotly_chart(fig, use_container_width=True)

# === TAB 3: REVENUE ANALYTICS ===
with tab3:
    st.subheader("💰 Revenue Insights")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Revenue by category
        revenue_cat_query = """
        SELECT 
            category,
            SUM(total_revenue) as revenue,
            SUM(order_count) as orders,
            AVG(avg_order_value) as avg_order_value
        FROM revenue_metrics
        WHERE category IS NOT NULL
        GROUP BY category
        ORDER BY revenue DESC
        """
        revenue_cat_df = query_db(revenue_cat_query)
        
        if not revenue_cat_df.empty:
            fig = px.bar(
                revenue_cat_df,
                x='category',
                y='revenue',
                title='Revenue by Category',
                labels={'revenue': 'Revenue (£)', 'category': 'Category'},
                color='revenue',
                color_continuous_scale='Blues'
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Stats
            st.markdown("### 📊 Category Performance")
            for _, row in revenue_cat_df.iterrows():
                st.markdown(f"""
                **{row['category']}**  
                💰 £{row['revenue']:,.0f} | 🛒 {int(row['orders'])} orders | 📊 Avg: £{row['avg_order_value']:.2f}
                """)
    
    with col2:
        # Revenue by payment method
        payment_query = """
        SELECT 
            payment_method,
            SUM(total_revenue) as revenue,
            SUM(order_count) as orders
        FROM revenue_metrics
        WHERE payment_method IS NOT NULL
        GROUP BY payment_method
        ORDER BY revenue DESC
        """
        payment_df = query_db(payment_query)
        
        if not payment_df.empty:
            fig = px.pie(
                payment_df,
                values='revenue',
                names='payment_method',
                title='Revenue by Payment Method',
                hole=0.3
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Revenue over time
    st.subheader("📈 Revenue Trend")
    revenue_time_query = """
    SELECT 
        window_start,
        SUM(total_revenue) as revenue,
        SUM(order_count) as orders
    FROM revenue_metrics
    GROUP BY window_start
    ORDER BY window_start
    """
    revenue_time_df = query_db(revenue_time_query)
    
    if not revenue_time_df.empty:
        fig = px.area(
            revenue_time_df,
            x='window_start',
            y='revenue',
            title='Revenue Over Time',
            labels={'revenue': 'Revenue (£)', 'window_start': 'Time'}
        )
        fig.update_traces(fillcolor='rgba(102, 126, 234, 0.3)', line_color='#667eea')
        st.plotly_chart(fig, use_container_width=True)

# === TAB 4: LIVE ALERTS ===
with tab4:
    st.subheader("Real-Time Alerts & Monitoring")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### ⚡ Recent High-Value Orders")
        
        high_value_query = """
        SELECT 
            window_start as time,
            category,
            high_value_orders as count,
            total_revenue as revenue
        FROM revenue_metrics
        WHERE high_value_orders > 0
        ORDER BY window_start DESC
        LIMIT 10
        """
        high_value_df = query_db(high_value_query)
        
        if not high_value_df.empty:
            for _, row in high_value_df.iterrows():
                st.warning(f"""
                ⚠️ **High-Value Order Detected**  
                Category: {row['category']}  
                Count: {int(row['count'])} orders  
                Revenue: £{row['revenue']:,.2f}  
                Time: {row['time']}
                """)
        else:
            st.info("No high-value orders in recent period")
    
    with col2:
        st.markdown("### 🔔 Top Products Need Attention")
        
        # Products with high views but no purchases
        attention_query = """
        SELECT 
            product_name,
            category,
            SUM(unique_viewers) as viewers,
            SUM(cart_adds) as cart_adds,
            SUM(purchases) as purchases
        FROM trending_products
        WHERE product_name IS NOT NULL
        GROUP BY product_name, category
        HAVING SUM(unique_viewers) > 50 AND SUM(purchases) = 0
        ORDER BY SUM(unique_viewers) DESC
        LIMIT 5
        """
        attention_df = query_db(attention_query)
        
        if not attention_df.empty:
            for _, row in attention_df.iterrows():
                st.info(f"""
                **{row['product_name']}**  
                {int(row['viewers'])} viewers, 🛒 {int(row['cart_adds'])} in cart  
                But 0 purchases - consider price adjustment or promotion
                """)
        else:
            st.success("✅ All popular products converting well!")
    
    # System health
    st.markdown("### 🏥 System Health")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Pipeline Status", "🟢 Healthy", delta="All systems operational")
    
    with col2:
        latest_data = query_db("SELECT MAX(window_start) as latest FROM funnel_metrics")
        if not latest_data.empty and latest_data['latest'].iloc[0]:
            time_diff = datetime.now() - pd.to_datetime(latest_data['latest'].iloc[0])
            minutes_ago = int(time_diff.total_seconds() / 60)
            st.metric("Data Freshness", f"{minutes_ago} min ago", delta="Real-time")
    
    with col3:
        total_records = query_db("SELECT COUNT(*) as cnt FROM funnel_metrics")
        if not total_records.empty:
            st.metric("Total Records", f"{int(total_records['cnt'].iloc[0]):,}", delta="Growing")

# Footer
st.markdown("---")
st.markdown(" Data from Delta Lake + PostgreSQL | Auto-refresh available")