# dashboard.py

import streamlit as st
import pandas as pd
import plotly.express as px

# 1. db_handler.py 파일에서 데이터 조회 함수를 가져옵니다.
from db_handler import fetch_hourly_volume

# --- 페이지 기본 설정 ---
st.set_page_config(
    page_title="가상자산 거래대금 대시보드",
    page_icon="📈",
    layout="wide",
)

# --- 데이터 로딩 ---
# Streamlit의 캐싱 기능을 사용해 10분(600초)에 한 번만 DB에서 데이터를 새로 가져옵니다.
# 이렇게 하면 페이지를 새로고침할 때마다 DB에 부담을 주지 않아 빠릅니다.
@st.cache_data(ttl=600)
def load_data(hours):
    df = fetch_hourly_volume(hours=hours)
    if not df.empty:
        # DB에서 가져온 KST 시간 문자열을 datetime 객체로 변환하기만 하면 됩니다.
        # 시간대 변환(localize, convert) 로직은 모두 제거합니다.
        df['datetime_kst'] = pd.to_datetime(df['datetime_kst'])
        
        df['traded_price'] = pd.to_numeric(df['traded_price'])
    return df

# --- 사이드바 ---
st.sidebar.header("🗓️ 데이터 조회 설정")
hours_to_fetch = st.sidebar.slider(
    "조회할 시간 범위(시간)",
    min_value=1,      # 최소 1시간
    max_value=168,    # 최대 1주일 (24 * 7)
    value=24,         # 기본값 24시간
    step=1
)

# --- 메인 대시보드 ---
st.title("📈 업비트 & 빗썸 시간별 거래대금 대시보드")

# 데이터 로딩 실행
all_df = load_data(hours=hours_to_fetch)

# 데이터가 성공적으로 로드되었는지 확인
if all_df.empty:
    st.error("데이터를 불러오는 데 실패했거나, 선택된 기간에 데이터가 없습니다.")
else:
    # 1. 시간별 전체 거래대금 비교 (막대 차트)
    st.header("거래소별 전체 거래대금 추이")

    # --- 이 부분을 추가! ---
    # 'datetime_kst' 컬럼을 '시간' 단위로 그룹화하기 위해 날짜와 시간까지만 남깁니다.
    # 예: 2025-09-22 13:59:15 -> 2025-09-22 13:00:00
    all_df['datetime_hourly'] = all_df['datetime_kst'].dt.floor('h')
    # -----------------------

    # 그룹화 기준을 'datetime_hourly'로 변경합니다.
    total_volume_df = all_df.groupby(['datetime_hourly', 'exchange'])['traded_price'].sum().reset_index()

    fig_bar = px.bar(
        total_volume_df,
        x='datetime_hourly', # x축 기준을 새로운 컬럼으로 변경
        y='traded_price',
        color='exchange',
        barmode='group',
        color_discrete_map={
            'upbit': 'royalblue',
            'bithumb': '#FF7F0E'
        },
        title='시간별 거래대금 추이',
        labels={'datetime_hourly': '시간', 'traded_price': '거래대금(원)', 'exchange': '거래소'}
    )
    st.plotly_chart(fig_bar, use_container_width=True)
#---------------------------------------------------------------------------------------------------------------------------------------------
    # 2. 시간별 전체 거래대금 표
    # 그래프와 표 사이에 구분선 추가
    st.divider()

    # 표를 위한 부제목 추가
    st.subheader("시간별 거래대금 상세 데이터")

    # 데이터를 표로 보기 좋게 재구성 (피벗 테이블)
    #    - index: 표의 행 (시간)
    #    - columns: 표의 열 (거래소)
    #    - values: 표에 채워질 값 (거래대금)
    table_df = total_volume_df.pivot_table(
        index='datetime_hourly', 
        columns='exchange', 
        values='traded_price'
    ).fillna(0) # 데이터가 없는 칸은 0으로 채움

    # 최신 시간이 맨 위로 오도록 내림차순 정렬
    table_df.sort_index(ascending=False, inplace=True)

    # 숫자에 콤마(,)와 '원' 단위를 붙여서 보기 좋게 출력
    st.dataframe(
        table_df.style.format("{:,.0f}원"),
        use_container_width=True
    )
#---------------------------------------------------------------------------------------------------------------------------------------------
    # 3. 최근 1시간 데이터 분석
    latest_time = all_df['datetime_kst'].max()
    st.header(f"최근 데이터 분석 ({latest_time.strftime('%Y-%m-%d %H:%M')})")

    latest_df = all_df[all_df['datetime_kst'] == latest_time]

    # 화면을 3개의 열로 분할 (파이차트, 업비트 Top5, 빗썸 Top5)
    col1, col2, col3 = st.columns([2, 2, 2]) # 파이차트가 조금 좁게, 나머지는 넓게

    with col1:
        # 거래소별 점유율 (파이 차트)
        st.subheader("거래대금 점유율")
        pie_df = latest_df.groupby('exchange')['traded_price'].sum().reset_index()
        fig_pie = px.pie(
            pie_df,
            names='exchange',
            color='exchange',
            color_discrete_map={
                'upbit': 'royalblue',
                'bithumb': '#FF7F0E'
            },
            values='traded_price',
            hole=0.4,
            title='거래소별 점유율'
        )
        # 차트의 여백을 줄여서 더 깔끔하게 만듭니다.
        fig_pie.update_layout(margin=dict(l=20, r=20, t=40, b=20))
        st.plotly_chart(fig_pie, use_container_width=True)

    with col2:
        # 거래대금 TOP 5 코인 (업비트)
        st.subheader("Upbit 거래대금 TOP 10")
        # 'latest_df'로 오타 수정
        top10_upbit_df = latest_df[latest_df['exchange'] == "upbit"].sort_values(by='traded_price', ascending=False).head(10)
        st.dataframe(top10_upbit_df[['market', 'traded_price']].style.format({"traded_price": "{:,.0f}원"}),
                     use_container_width=True)

    with col3:
        # 거래대금 TOP 5 코인 (빗썸)
        st.subheader("Bithumb 거래대금 TOP 10")
        # 'latest_df'로 오타 수정
        top10_bithumb_df = latest_df[latest_df['exchange'] == "bithumb"].sort_values(by='traded_price', ascending=False).head(10)
        st.dataframe(top10_bithumb_df[['market', 'traded_price']].style.format({"traded_price": "{:,.0f}원"}),
                     use_container_width=True)

    
    # 4. 전체 원본 데이터 보기
    with st.expander("전체 원본 데이터 보기"):
        st.dataframe(all_df)