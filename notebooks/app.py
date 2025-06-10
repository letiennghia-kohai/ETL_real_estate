import streamlit as st
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, LabelEncoder, OneHotEncoder
from sklearn.compose import ColumnTransformer
import pickle
import joblib

# Cấu hình trang
st.set_page_config(
    page_title="Dự đoán giá bất động sản",
    page_icon="🏠",
    layout="wide"
)

# Tiêu đề ứng dụng
st.title("🏠 Ứng dụng dự đoán giá bất động sản")
st.markdown("---")

# Sidebar để tải mô hình
st.sidebar.header("Cài đặt mô hình")
# st.sidebar.info("Lưu ý: Trong môi trường thực tế, bạn cần tải mô hình đã được huấn luyện từ file pickle.")

# Hàm tạo mô hình demo (trong thực tế bạn sẽ load từ file)
@st.cache_resource
def load_model():
    """
    Trong thực tế, bạn sẽ load mô hình đã được huấn luyện:
    model = joblib.load('rf_model.pkl')
    return model
    """
    # Tạo mô hình demo để minh họa
    from sklearn.preprocessing import LabelEncoder
    
    # Tạo preprocessor giả lập
    numeric_features = ['area_value', 'bedrooms_value', 'bathrooms_value', 'floors_value',
                       'road_width_value', 'house_front_value', 'width_value', 'length_value',
                       'description_length', 'description_word_count', 'amenities_count']
    
    categorical_features = ['post_type', 'property_type', 'direction', 'balcony_direction', 'city', 'district']
    
    preprocessor = ColumnTransformer(
        transformers=[
            ('num', StandardScaler(), numeric_features),
            ('cat', OneHotEncoder(drop='first', handle_unknown='ignore'), categorical_features)
        ])
    
    model = Pipeline([
        ('preprocessor', preprocessor),
        ('regressor', RandomForestRegressor(n_estimators=100, random_state=42))
    ])
    
    return model, numeric_features, categorical_features

# Load mô hình
model, numeric_features, categorical_features = load_model()

# Layout chính
col1, col2 = st.columns([2, 1])

with col1:
    st.header("Nhập thông tin bất động sản")
    
    # Form nhập liệu
    with st.form("prediction_form"):
        # Thông tin cơ bản
        st.subheader("Thông tin cơ bản")
        col_basic1, col_basic2 = st.columns(2)
        
        with col_basic1:
            area_value = st.number_input("Diện tích (m²)", min_value=0.0, value=50.0, step=1.0)
            bedrooms_value = st.number_input("Số phòng ngủ", min_value=0.0, value=2.0, step=1.0)
            bathrooms_value = st.number_input("Số phòng tắm", min_value=0.0, value=1.0, step=1.0)
            floors_value = st.number_input("Số tầng", min_value=0.0, value=1.0, step=1.0)
        
        with col_basic2:
            width_value = st.number_input("Chiều rộng (m)", min_value=0.0, value=5.0, step=0.1)
            length_value = st.number_input("Chiều dài (m)", min_value=0.0, value=10.0, step=0.1)
            house_front_value = st.number_input("Mặt tiền (m)", min_value=0.0, value=4.0, step=0.1)
            road_width_value = st.number_input("Độ rộng đường (m)", min_value=0.0, value=3.0, step=0.1)
        
        # Thông tin mô tả
        st.subheader("Thông tin mô tả")
        col_desc1, col_desc2 = st.columns(2)
        
        with col_desc1:
            description_length = st.number_input("Độ dài mô tả (ký tự)", min_value=0, value=500, step=1)
            description_word_count = st.number_input("Số từ trong mô tả", min_value=0, value=50, step=1)
        
        with col_desc2:
            amenities_count = st.number_input("Số tiện ích", min_value=0, value=5, step=1)
        
        # Thông tin phân loại
        st.subheader("Thông tin phân loại")
        col_cat1, col_cat2 = st.columns(2)
        
        with col_cat1:
            post_type = st.selectbox("Loại tin đăng", ["Bán", "Cho thuê"])
            property_type = st.selectbox("Loại bất động sản", 
                                       ["Nhà riêng", "Chung cư", "Biệt thự", "Nhà mặt phố", "Đất nền"])
            direction = st.selectbox("Hướng nhà", 
                                   ["Đông", "Tây", "Nam", "Bắc", "Đông Nam", "Đông Bắc", "Tây Nam", "Tây Bắc"])
        
        with col_cat2:
            balcony_direction = st.selectbox("Hướng ban công", 
                                           ["Đông", "Tây", "Nam", "Bắc", "Đông Nam", "Đông Bắc", "Tây Nam", "Tây Bắc"])
            city = st.selectbox("Thành phố", ["Hà Nội", "TP. Hồ Chí Minh", "Đà Nẵng", "Hải Phòng", "Cần Thơ"])
            district = st.selectbox("Quận/Huyện", 
                                  ["Quận 1", "Quận 2", "Quận 3", "Ba Đình", "Hoàn Kiếm", "Cầu Giấy", "Thanh Xuân"])
        
        # Nút dự đoán
        submitted = st.form_submit_button("🔮 Dự đoán giá", use_container_width=True)

with col2:
    st.header("Kết quả dự đoán")
    
    if submitted:
        # Tạo DataFrame từ dữ liệu nhập vào
        input_data = pd.DataFrame({
            'area_value': [area_value],
            'post_type': [post_type],
            'property_type': [property_type],
            'is_available': [True],  # Giả định luôn có sẵn
            'description_length': [description_length],
            'description_word_count': [description_word_count],
            'bedrooms_value': [bedrooms_value],
            'bathrooms_value': [bathrooms_value],
            'floors_value': [floors_value],
            'direction': [direction],
            'balcony_direction': [balcony_direction],
            'road_width_value': [road_width_value],
            'house_front_value': [house_front_value],
            'width_value': [width_value],
            'length_value': [length_value],
            'city': [city],
            'district': [district],
            'amenities_count': [amenities_count]
        })
        
        try:
            # Dự đoán (lưu ý: trong demo này sẽ tạo giá trị ngẫu nhiên)
            # prediction = model.predict(input_data)[0]
            
            # Tạo dự đoán demo dựa trên diện tích và vị trí
            base_price = area_value * 50_000_000  # 50 triệu/m²
            city_multiplier = {"Hà Nội": 1.2, "TP. Hồ Chí Minh": 1.5, "Đà Nẵng": 0.8, "Hải Phòng": 0.7, "Cần Thơ": 0.6}
            property_multiplier = {"Biệt thự": 1.5, "Nhà mặt phố": 1.3, "Nhà riêng": 1.0, "Chung cư": 0.8, "Đất nền": 0.6}
            
            prediction = base_price * city_multiplier.get(city, 1.0) * property_multiplier.get(property_type, 1.0)
            
            # Hiển thị kết quả
            st.success("Dự đoán thành công!")
            
            # Hiển thị giá dự đoán
            st.metric(
                label="Giá dự đoán",
                value=f"{prediction:,.0f} VNĐ",
                delta=f"{prediction/area_value:,.0f} VNĐ/m²"
            )
            
            # Thống kê bổ sung
            st.subheader("Thông tin bổ sung")
            st.write(f"**Diện tích:** {area_value} m²")
            st.write(f"**Giá trên m²:** {prediction/area_value:,.0f} VNĐ/m²")
            st.write(f"**Loại BDS:** {property_type}")
            st.write(f"**Vị trí:** Cầu Giấy, {city}")
            
            # Biểu đồ so sánh (demo)
            chart_data = pd.DataFrame({
                'Khu vực': ['Trung bình thị trường', 'Dự đoán của bạn'],
                'Giá (tỷ VNĐ)': [prediction * 0.8 / 1_000_000_000, prediction / 1_000_000_000]
            })
            
            st.bar_chart(chart_data.set_index('Khu vực'))
            
        except Exception as e:
            st.error(f"Lỗi khi dự đoán: {str(e)}")
            st.info("Lưu ý: Đây là ứng dụng demo. Trong thực tế, bạn cần load mô hình đã được huấn luyện.")

# Phần thông tin về mô hình
st.markdown("---")
st.header("Thông tin về mô hình")

col_info1, col_info2 = st.columns(2)

with col_info1:
    st.subheader("Độ chính xác mô hình")
    st.write("- **RMSE:** 0.36")
    st.write("- **R² Score:** 0.6112")
    st.write("- **Mô hình:** Random Forest Regressor")

with col_info2:
    st.subheader("Các yếu tố ảnh hưởng")
    st.write("- Diện tích và vị trí")
    st.write("- Số phòng ngủ, phòng tắm")
    st.write("- Loại bất động sản")
    st.write("- Tiện ích và mô tả")

# Hướng dẫn sử dụng
with st.expander("📖 Hướng dẫn sử dụng"):
    st.markdown("""
    ### Cách sử dụng ứng dụng:
    1. **Nhập thông tin bất động sản** vào các trường bên trái
    2. **Click nút "Dự đoán giá"** để nhận kết quả
    3. **Xem kết quả** ở cột bên phải với giá dự đoán và thông tin chi tiết
    
    ### Lưu ý:
    - Đây là ứng dụng demo với mô hình được tạo để minh họa
    - Trong thực tế, bạn cần load mô hình đã được huấn luyện từ file pickle
    - Kết quả dự đoán chỉ mang tính chất tham khảo
    """)

# Footer
st.markdown("---")
st.markdown("*Ứng dụng được phát triển bằng Streamlit - Demo dự đoán giá bất động sản*")