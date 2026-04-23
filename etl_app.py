import streamlit as st
import pandas as pd
import sqlite3
import time

# Configuración de la página
st.set_page_config(
    page_title="Simulador ETL - MBA",
    page_icon="📊",
    layout="wide"
)

# Estilos personalizados para darle un toque premium
st.markdown("""
    <style>
    .main-title { font-size: 48px; font-weight: bold; color: #1E3A8A; }
    .subtitle { font-size: 18px; color: #555555; margin-bottom: 20px; }
    .step-header { font-size: 24px; font-weight: bold; color: #0D9488; margin-top: 10px; }
    </style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
#  TÍTULO PRINCIPAL
# ─────────────────────────────────────────────
st.markdown('<p class="main-title">🚀 Simulación ETL: Empresa de Retail "Eleana S.A.C."</p>', unsafe_allow_html=True)
st.markdown('<p class="subtitle">Ejemplo práctico para el curso "Business Intelligence & Business Analytics". Observa cómo fluyen los datos desde los sistemas transaccionales hasta la toma de decisiones.</p>', unsafe_allow_html=True)

# ─────────────────────────────────────────────
#  GENERACIÓN DE DATOS (En memoria)
# ─────────────────────────────────────────────
@st.cache_data
def generar_datos_crudos():
    # Ventas (ERP)
    ventas_raw = pd.DataFrame({
        "id_venta": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "fecha": ["2024-01-15", "2024-02-20", "2024-02-28", "2024-03-05", "2024-03-10", "2024-04-01", "2024-04-15", "2024-05-05", "2024-05-20", "2024-06-01"],
        "id_cliente": [101, 102, 103, 101, 104, 102, 105, 103, None, 101],
        "id_producto": ["P01", "P02", "P01", "P03", "P02", "P01", "p03", "P02", "P01", "P04"],
        "cantidad": [2, 1, 3, 1, 2, 4, 1, 2, 3, 1],
        "monto_usd": [150.00, 89.99, 225.00, 110.00, 179.98, 300.00, 110.00, 179.98, 225.00, -50.00],
        "region": ["LIMA", "Arequipa", "lima", "CUSCO", "LIMA", "arequipa", "Cusco", "LIMA", "LIMA", "LIMA"]
    })
    
    # Clientes (CRM)
    clientes_raw = pd.DataFrame({
        "id_cliente": [101, 102, 103, 104, 105],
        "nombre": ["Ana Torres", "Carlos Ríos", "María Gómez", "Pedro Salas", "Lucía Vega"],
        "segmento": ["Premium", "Estándar", "Premium", "Estándar", "Premium"],
        "ciudad": ["Lima", "Arequipa", "Lima", "Cusco", "Lima"]
    })
    
    # Productos (Catálogo)
    productos_raw = pd.DataFrame({
        "id_producto": ["P01", "P02", "P03", "P04"],
        "nombre": ["Laptop Empresarial", "Mouse Inalámbrico", "Teclado Mecánico", "Monitor 24\""],
        "categoria": ["Hardware", "Periféricos", "Periféricos", "Hardware"],
        "precio_unit": [75.00, 89.99, 110.00, 250.00]
    })
    
    return ventas_raw, clientes_raw, productos_raw

ventas_raw, clientes_raw, productos_raw = generar_datos_crudos()

# Inicialización de estados de sesión
if "transform_lista" not in st.session_state:
    st.session_state["transform_lista"] = False
if "carga_lista" not in st.session_state:
    st.session_state["carga_lista"] = False

# Creación de Pestañas
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📥 1. EXTRACT", 
    "🔧 2. TRANSFORM", 
    "🏛️ 3. LOAD", 
    "📊 4. BI DASHBOARD",
    "⚖️ Comparativa de Herramientas"
])

# ══════════════════════════════════════════════
#  TAB 1: EXTRACT
# ══════════════════════════════════════════════
with tab1:
    st.markdown('<p class="step-header">Extracción de Sistemas Origen</p>', unsafe_allow_html=True)
    st.write("Simulamos la conexión a 3 sistemas diferentes de la empresa para extraer los datos en bruto.")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.info("🛒 **Sistema ERP (Ventas)**")
        st.dataframe(ventas_raw, use_container_width=True)
        st.warning("⚠️ Nota los valores nulos en clientes, montos negativos e inconsistencias en la región.")
        
    with col2:
        st.success("👥 **Sistema CRM (Clientes)**")
        st.dataframe(clientes_raw, use_container_width=True)
        
    with col3:
        st.success("📦 **Catálogo Interno (Productos)**")
        st.dataframe(productos_raw, use_container_width=True)


# ══════════════════════════════════════════════
#  TAB 2: TRANSFORM
# ══════════════════════════════════════════════
with tab2:
    st.markdown('<p class="step-header">Limpieza, Estandarización y Enriquecimiento</p>', unsafe_allow_html=True)
    st.write("Aquí es donde ocurre la magia del Data Engineering. Convertimos datos sucios en información confiable.")
    
    # BOTÓN PARA EJECUTAR LA TRANSFORMACIÓN
    if st.button("Ejecutar Transformación 🔧", type="primary"):
        with st.spinner("Limpiando nulos, estandarizando texto y calculando métricas..."):
            time.sleep(2)  # Efecto dramático para la clase
            st.session_state["transform_lista"] = True
            
    st.write("---")
    
    if st.session_state["transform_lista"]:
        # Aplicar transformaciones paso a paso
        ventas = ventas_raw.copy()
        
        # T1 y T2: Estandarización
        ventas["region"] = ventas["region"].str.upper().str.strip()
        ventas["id_producto"] = ventas["id_producto"].str.upper().str.strip()
        
        # T3: Eliminar montos negativos
        eliminados_monto = len(ventas[ventas["monto_usd"] < 0])
        ventas = ventas[ventas["monto_usd"] >= 0]
        
        # T4: Manejar nulos
        eliminados_nulos = ventas["id_cliente"].isnull().sum()
        ventas = ventas.dropna(subset=["id_cliente"])
        ventas["id_cliente"] = ventas["id_cliente"].astype(int)
        
        # T5: Fechas
        ventas["fecha"] = pd.to_datetime(ventas["fecha"])
        ventas["mes"] = ventas["fecha"].dt.month
        ventas["trimestre"] = ventas["fecha"].dt.quarter
        ventas["anio"] = ventas["fecha"].dt.year
        
        # T6 y T7: Joins y KPIs
        ventas_enriq = ventas.merge(clientes_raw, on="id_cliente", how="left")
        ventas_enriq = ventas_enriq.merge(productos_raw, on="id_producto", how="left")
        ventas_enriq["ingreso_total"] = ventas_enriq["cantidad"] * ventas_enriq["precio_unit"]
        ventas_enriq["ticket_promedio"] = ventas_enriq["ingreso_total"] / ventas_enriq["cantidad"]
        
        # Guardamos en sesión para usarlo en el tab de carga
        st.session_state["ventas_enriq"] = ventas_enriq
        
        st.success("🎉 ¡Proceso de transformación completado!")
        
        # Mostrar resumen de la transformación en métricas
        m1, m2, m3, m4 = st.columns(4)
        m1.metric(label="Registros Originales", value=len(ventas_raw))
        m2.metric(label="Eliminados (Negativos)", value=eliminados_monto, delta=f"-{eliminados_monto}", delta_color="inverse")
        m3.metric(label="Eliminados (Sin ID Cliente)", value=eliminados_nulos, delta=f"-{eliminados_nulos}", delta_color="inverse")
        m4.metric(label="Registros Limpios Finales", value=len(ventas_enriq))
        
        st.subheader("Dataset Resultante (Listo para Analítica)")
        cols_mostrar = ["id_venta", "fecha", "nombre_x", "segmento", "nombre_y", "categoria", "cantidad", "ingreso_total", "region", "trimestre"]
        df_final_mostrar = ventas_enriq[cols_mostrar].rename(columns={"nombre_x": "cliente", "nombre_y": "producto"})
        
        st.dataframe(df_final_mostrar, use_container_width=True)
    else:
        st.info("💡 Presiona el botón de arriba para iniciar la transformación de los datos.")


# ══════════════════════════════════════════════
#  TAB 3: LOAD
# ══════════════════════════════════════════════
with tab3:
    st.markdown('<p class="step-header">Carga al Data Warehouse</p>', unsafe_allow_html=True)
    st.write("Simulamos el volcado de las dimensiones y la tabla de hechos a nuestra base de datos analítica.")
    
    # VALIDACIÓN: No puedes cargar si no has transformado
    if not st.session_state["transform_lista"]:
        st.warning("⚠️ ¡Alto! No puedes cargar datos en el Data Warehouse que aún no han sido transformados. Por favor, ve a la pestaña **2. TRANSFORM** y ejecuta el proceso.")
    else:
        boton_carga = st.button("Iniciar Carga en Data Warehouse 🏛️", type="primary")
        
        if boton_carga:
            with st.spinner("Conectando y cargando datos en SQLite simulado..."):
                time.sleep(1.5) 
                
                # Proceso de carga real en base de datos temporal SQLite
                conn = sqlite3.connect(":memory:") 
                
                # Obtener el dataframe transformado guardado en sesión
                ventas_enriq = st.session_state["ventas_enriq"]
                
                # Guardamos tablas
                ventas_final_sql = ventas_enriq.rename(columns={"nombre_x": "cliente", "nombre_y": "producto"})
                ventas_final_sql.to_sql("fact_ventas", conn, if_exists="replace", index=False)
                clientes_raw.to_sql("dim_clientes", conn, if_exists="replace", index=False)
                productos_raw.to_sql("dim_productos", conn, if_exists="replace", index=False)
                
                # Guardamos la conexión en el estado de la sesión para usarla en el Dashboard
                st.session_state["db_conn"] = conn
                st.session_state["carga_lista"] = True
                
            st.success("🎉 ¡Carga completada con éxito! Las tablas `fact_ventas`, `dim_clientes` y `dim_productos` están listas.")
            st.balloons()
        else:
            if not st.session_state["carga_lista"]:
                st.warning("Haz clic en el botón superior para realizar la carga de los datos transformados.")


# ══════════════════════════════════════════════
#  TAB 4: BI DASHBOARD
# ══════════════════════════════════════════════
with tab4:
    st.markdown('<p class="step-header">Business Intelligence Layer (Data Visualization)</p>', unsafe_allow_html=True)
    
    if st.session_state["carga_lista"]:
        conn = st.session_state["db_conn"]
        
        # Consultas de negocio directo desde la BD
        q_trimestre = pd.read_sql("SELECT trimestre, SUM(ingreso_total) as ingreso_total_usd FROM fact_ventas GROUP BY trimestre", conn)
        q_region = pd.read_sql("SELECT region, SUM(ingreso_total) as ingreso_total_usd FROM fact_ventas GROUP BY region", conn)
        q_top_clientes = pd.read_sql("SELECT cliente, SUM(ingreso_total) as total FROM fact_ventas GROUP BY cliente ORDER BY total DESC", conn)
        q_categoria = pd.read_sql("SELECT categoria, SUM(cantidad) as unidades FROM fact_ventas GROUP BY categoria", conn)
        
        # Fila 1 de Dashboards
        c1, c2 = st.columns(2)
        
        with c1:
            st.subheader("📈 Ingresos por Trimestre")
            st.bar_chart(data=q_trimestre.set_index("trimestre"), use_container_width=True)
            
        with c2:
            st.subheader("🗺️ Distribución de Ingresos por Región")
            st.bar_chart(data=q_region.set_index("region"), color="#0D9488", use_container_width=True)
            
        # Fila 2 de Dashboards
        c3, c4 = st.columns(2)
        
        with c3:
            st.subheader("👑 Top Clientes por Ingreso")
            st.dataframe(q_top_clientes, use_container_width=True, hide_index=True)
            
        with c4:
            st.subheader("📦 Unidades Vendidas por Categoría")
            st.bar_chart(data=q_categoria.set_index("categoria"), color="#F59E0B", use_container_width=True)
            
    else:
        st.info("Primero debes ejecutar el paso de 'LOAD' (Carga) en la pestaña anterior para poder visualizar el Dashboard con la base de datos final.")


# ══════════════════════════════════════════════
#  TAB 5: COMPARATIVA
# ══════════════════════════════════════════════
with tab5:
    st.markdown('<p class="step-header">Comparativo de Herramientas ETL</p>', unsafe_allow_html=True)
    st.write("Un resumen para orientar a los futuros directivos sobre qué herramienta elegir según el caso de uso.")
    
    comparativa = {
        "Herramienta": ["PYTHON (Este simulador)", "SQL", "ALTERYX"],
        "Perfil ideal": ["Data Engineers / Científicos de Datos", "Analistas de Datos / BI", "Analistas de Negocio (Business Users)"],
        "Pros": ["Flexibilidad total, manejo de datos masivos, gratis.", "Estándar universal, súper veloz en bases de datos.", "No-code / Low-code, intuitivo, amigable."],
        "Contras": ["Curva de aprendizaje de código alta.", "Limitado para datos no estructurados o APIs.", "Costo de licencia muy elevado."]
    }
    
    st.table(pd.DataFrame(comparativa))