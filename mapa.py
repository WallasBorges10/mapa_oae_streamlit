import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import matplotlib.pyplot as plt
import folium
import streamlit as st
from streamlit_folium import folium_static
from streamlit import set_page_config
from streamlit_searchbox import st_searchbox
import webbrowser
import fiona
import zipfile
import io
import tempfile
import os

# PARTE 1 - TRANSFORMAÇÃO DE DADOS E MAPA

@st.cache_data
def load_data(uploaded_files):
    # Verificar se todos os arquivos necessários foram carregados
    required_files = ['base_oae_colep', 'SNV_202501A', 'BR_UF_2022']
    for file in required_files:
        if file not in uploaded_files:
            st.error(f"Arquivo obrigatório não encontrado: {file}")
            st.stop()
    
    try:
        # 1. Carregando os dados dos arquivos enviados
        v_oae_v2 = pd.read_excel(uploaded_files['base_oae_colep'])
        
        # Processar o shapefile SNV
        with zipfile.ZipFile(uploaded_files['SNV_202501A'], 'r') as z:
            # Encontrar o arquivo .shp dentro do ZIP
            shp_file = [f for f in z.namelist() if f.endswith('.shp')][0]
            # Extrair todos os arquivos para um diretório temporário
            temp_dir = tempfile.mkdtemp()
            z.extractall(temp_dir)
            # Carregar o shapefile usando o caminho completo
            v_snv_2025 = gpd.read_file(os.path.join(temp_dir, shp_file))
              
        # Processar o shapefile BR_UF
        with zipfile.ZipFile(uploaded_files['BR_UF_2022'], 'r') as z:
            # Encontrar o arquivo .shp dentro do ZIP
            shp_file = [f for f in z.namelist() if f.endswith('.shp')][0]
            # Extrair todos os arquivos para um diretório temporário
            temp_dir = tempfile.mkdtemp()
            z.extractall(temp_dir)
            # Carregar o shapefile usando o caminho completo
            v_uf = gpd.read_file(os.path.join(temp_dir, shp_file))

        # Restante do processamento...
        v_oae_v2['geometry'] = v_oae_v2.apply(lambda row: Point(row['longitude'], row['latitude']), axis=1)
        v_oae_v2 = gpd.GeoDataFrame(v_oae_v2, geometry='geometry', crs=v_snv_2025.crs)
        v_oae_v2 = v_oae_v2.to_crs(epsg=5880)
        v_snv_2025 = v_snv_2025.to_crs(epsg=5880)
        v_uf = v_uf.to_crs(epsg=5880)

        colunas_snv = ['vl_codigo', 'ds_tipo_ad', 'ds_jurisdi','ul','versao_snv', 'geometry']

        # 3. Spatial join com buffer de 250m (ST_DWithin)
        df_merged = gpd.sjoin(v_oae_v2, v_snv_2025[colunas_snv], how="left", predicate='dwithin', distance=250, lsuffix='1', rsuffix='2')

        df_merged = df_merged.drop(columns='index_2')
        v_uf.rename(columns={
            'SIGLA_UF': 'uf'
        }, inplace=True)
        v_uf = gpd.GeoDataFrame(v_uf, geometry='geometry')
        df_merged = gpd.sjoin(df_merged, v_uf[['uf', 'geometry']], how="left", predicate='dwithin', distance=500, lsuffix='1', rsuffix='2')
        del df_merged['index_2']
        df_merged['cod_sgo'] = df_merged['cod_sgo'].astype(str).str.zfill(6)
        df_merged['br'] = df_merged['br'].astype(str).str.zfill(3)

        # 4. Agrupamento similar ao CTE1
        agg_funcs = {
            'descr_obra': lambda x: ';'.join(set(x.dropna().astype(str))),
            'br': lambda x: ';'.join(set(x.dropna().apply(lambda y: str(y).zfill(3)))),
            'uf_1': lambda x: ';'.join(set(x.dropna().astype(str))),
            'ul_1': lambda x: ';'.join(set(x.dropna().astype(str))),
            'extens_m': lambda x: ';'.join(set(x.dropna().astype(str))),
            'largura_m': lambda x: ';'.join(set(x.dropna().astype(str))),
            'tipo_estrutura': lambda x: ';'.join(set(x.dropna().astype(str))),
            'tipo_obra': lambda x: ';'.join(set(x.dropna().astype(str))),
            'origem_cadastro': lambda x: ';'.join(set(x.dropna().astype(str))),
            'latitude': lambda x: ';'.join(set(x.dropna().astype(str))),
            'longitude': lambda x: ';'.join(set(x.dropna().astype(str))),
            'uf_2': lambda x: ';'.join(set(x.dropna().astype(str))),
            'vl_codigo': lambda x: ';'.join(set(x.dropna().astype(str))),
            'ds_tipo_ad': lambda x: ';'.join(set(x.dropna().astype(str))),
            'ds_jurisdi': lambda x: ';'.join(set(x.dropna().astype(str))),
            'ul_2': lambda x: ';'.join(set(x.dropna().astype(str))),
        }

        df_grouped = df_merged.groupby(['cod_sgo', 'geometry']).agg(agg_funcs).reset_index()

        # 5. Join com v_oae_sgo
        v_oae_sgo['Código'] = v_oae_sgo['Código'].astype(str).str.zfill(6)
        df_grouped['cod_sgo'] = df_grouped['cod_sgo'].astype(str).str.zfill(6)

        df_merged = pd.merge(df_grouped, v_oae_sgo[['Código', 'PNV','Nota']], left_on='cod_sgo', right_on='Código', how='left')
        df_merged = df_merged.rename(columns={'Nota': 'nota_sgo', 'PNV': 'sgo_pnv'})
        del df_merged['Código']
        
        # 6. Calculando conflitos
        df_merged['conflitos'] = df_merged.apply(
            lambda row: 'Sim' if any(';' in str(row[col]) for col in ['ds_tipo_ad', 'ds_jurisdi', 'ul_2', 'uf_2']) else 'Não',
            axis=1
        )
        df_merged.rename(columns={
            'uf_1': 'uf',
            'ul_1': 'ul' 
            }, inplace=True)

        # 7. Join df_final com v_snv_2025 novamente para trazer campos do PNV
        df_final = pd.merge(df_merged, v_snv_2025[['vl_codigo', 'ds_tipo_ad', 'ds_jurisdi', 'ul']], left_on='sgo_pnv', right_on='vl_codigo', how='left', suffixes=('','_pnv'))
        df_final = gpd.GeoDataFrame(df_final, geometry='geometry')
        df_oae = df_final.copy()
        df_snv = v_snv_2025.copy()
        
        # Simplificar geometria
        df_snv['geometry'] = df_snv['geometry'].simplify(tolerance=10, preserve_topology=True)

        # Criação da coluna tipo_conflito
        df_oae.loc[df_oae['uf_2'].str.contains(';', case=False, na=False), 'conflito_divisa'] = 'Divisa'
        df_oae.loc[df_oae['ds_tipo_ad'].str.contains(';', case=False, na=False), 'conflito_administracao'] = 'Administração'
        df_oae.loc[df_oae['ds_jurisdi'].str.contains(';', case=False, na=False), 'conflito_jurisdicao'] = 'Jurisdição'
        df_oae.loc[df_oae['ul_2'].str.contains(';', case=False, na=False), 'conflito_unidade_local'] = 'UnidadeLocal'

        # Concatenação da coluna tipo_conflito
        df_oae['tipo_conflito'] = df_oae[
            ['conflito_divisa', 'conflito_administracao', 'conflito_jurisdicao', 'conflito_unidade_local']
        ].apply(lambda row: '; '.join(row.dropna()), axis=1)

        df_oae.drop(
            columns=['conflito_divisa', 'conflito_administracao', 'conflito_jurisdicao', 'conflito_unidade_local'],
            inplace=True
        )

        df_oae['tipo_conflito'] = df_oae['tipo_conflito'].replace('', None)

        # Substituição de valores vazios ou sem preenchimento
        df_oae['nota_sgo'] = df_oae['nota_sgo'].fillna('Sem nota')
        df_oae['tipo_obra'] = df_oae['tipo_obra'].replace('', '-')
        df_oae['ds_tipo_ad'] = df_oae['ds_tipo_ad'].replace('', None)

        # Criação da Coluna 'streetview_link'
        df_oae['latitude'] = df_oae['latitude'].str.split(';').str[0]
        df_oae['longitude'] = df_oae['longitude'].str.split(';').str[0]
        df_oae['streetview_link'] = df_oae.apply(
            lambda row: f"https://www.google.com/maps?q=&layer=c&cbll={row['latitude']},{row['longitude']}", 
            axis=1
        )

        return df_snv, df_oae
    
    except Exception as e:
        st.error(f"Erro ao processar os arquivos: {str(e)}")
        st.stop()

# PARTE 2 - STREAMLIT

# Interface do Streamlit
st.set_page_config(page_title="Mapa OAE", layout="wide")

st.title("Mapa OAE")

# Seção de upload de arquivos
st.sidebar.header("Upload de Arquivos Obrigatórios")

# Dicionário para armazenar os arquivos carregados
uploaded_files = {}

# Upload do arquivo Excel base_oae_colep
uploaded_excel = st.sidebar.file_uploader("Base OAE (Excel)", type=['xlsx'], key='base_oae_colep')
if uploaded_excel is not None:
    uploaded_files['base_oae_colep'] = uploaded_excel

# Upload do shapefile SNV (deve ser um zip)
uploaded_snv = st.sidebar.file_uploader("Shapefile SNV (ZIP contendo .shp, .dbf, etc)", type=['zip'], key='SNV_202501A')
if uploaded_snv is not None:
    uploaded_files['SNV_202501A'] = uploaded_snv

# Upload do shapefile BR_UF (deve ser um zip)
uploaded_uf = st.sidebar.file_uploader("Shapefile BR_UF (ZIP contendo .shp, .dbf, etc)", type=['zip'], key='BR_UF_2022')
if uploaded_uf is not None:
    uploaded_files['BR_UF_2022'] = uploaded_uf

# Verificar se todos os arquivos foram carregados antes de continuar
if len(uploaded_files) == 4:
    df_snv, df_oae = load_data(uploaded_files)
    
    # Restante do código (igual ao original)...
    # Função para buscar sugestões
    def search_oae(searchterm: str, df: pd.DataFrame) -> list[tuple[str, str]]:
        # Converte para string e remove valores nulos
        df = df.dropna(subset=['cod_sgo', 'descr_obra'])
        df['cod_sgo'] = df['cod_sgo'].astype(str)
        df['descr_obra'] = df['descr_obra'].astype(str)
        
        if not searchterm:
            return []
        
        searchterm = searchterm.lower()
        
        # Cria colunas combinadas para pesquisa nos dois formatos
        df['cod_desc'] = df['cod_sgo'] + " - " + df['descr_obra']
        df['desc_cod'] = df['descr_obra'] + " - " + df['cod_sgo']
        
        # Busca em todas as colunas relevantes
        mask = (df['cod_sgo'].str.lower().str.contains(searchterm)) | \
               (df['descr_obra'].str.lower().str.contains(searchterm)) | \
               (df['cod_desc'].str.lower().str.contains(searchterm)) | \
               (df['desc_cod'].str.lower().str.contains(searchterm))
        
        results = df.loc[mask]
        
        # Formata os resultados para exibição (usando o formato código - descrição)
        suggestions = [
            (f"{row['cod_sgo']} - {row['descr_obra']}", row['cod_sgo'])
            for _, row in results.iterrows()
        ]
        
        # Remove duplicados mantendo a primeira ocorrência
        seen = set()
        unique_suggestions = []
        for sug in suggestions:
            if sug[0] not in seen:
                seen.add(sug[0])
                unique_suggestions.append(sug)
        
        return unique_suggestions

    # Função para buscar sugestões com Street View
    def search_oae_with_streetview(searchterm: str):
        suggestions = search_oae(searchterm, df_oae)
        # Adiciona opção de Street View para cada sugestão
        return [(f"{label} (Abrir Street View)", value) for label, value in suggestions]

    # Variável para armazenar a obra selecionada
    if 'selected_obra_streetview' not in st.session_state:
        st.session_state.selected_obra_streetview = None

    # Função para abrir Street View
    def open_street_view(latitude, longitude):
        url = f"https://www.google.com/maps?q=&layer=c&cbll={latitude},{longitude}"
        webbrowser.open_new_tab(url)

    # Definir listas para tooltips
    lista_snv = ['vl_br', 'sg_uf', 'vl_codigo', 'ds_coinc', 'ds_tipo_ad', 'ds_jurisdi','ds_superfi','ul']
    lista_oae = ['cod_sgo', 'descr_obra', 'tipo_obra','nota_sgo', 'origem_cadastro', 'uf_2', 'vl_codigo', 'ds_tipo_ad','ds_jurisdi', 'ul_2']

    # Mapeamento de cores
    color_map = {
        'Convênio Adm.Federal/Estadual': 'cyan',
        'Federal': 'red',
        'Distrital': 'cyan',
        'Estadual': 'cyan',
        'Municipal': 'cyan',
        'Concessão Federal': 'cyan',
        'Convênio Adm.Federal/Municipal': 'cyan'
    }

    # Filtrar df_snv apenas para valores mapeados
    df_snv['ds_tipo_ad'] = df_snv['ds_tipo_ad'].astype(str)
    df_snv = df_snv[df_snv['ds_tipo_ad'].isin(color_map.keys())]

    # Função para criar o mapa
    def create_map(filtered_snv, filtered_oae, selected_point=None):
        m = filtered_snv.explore(
            column='ds_tipo_ad',
            cmap=list(color_map.values()),
            categories=list(color_map.keys()),
            style_kwds={"fillOpacity": 0.1},
            tooltip=lista_snv,
            categorical=True,
            name="Rodovias(SNV)",
            tiles="OpenStreetMap"
        )
        
        # Adicione este bloco para marcar o ponto selecionado
        if selected_point is not None:
            folium.Marker(
                location=[selected_point['latitude'], selected_point['longitude']],
                popup=f"OAE: {selected_point['cod_sgo']}",
                icon=folium.Icon(color='red', icon='info-sign')
            ).add_to(m)
        
        # Adicionar pontos OAE com tooltip personalizado
        for _, row in filtered_oae.iterrows():
            # Criar conteúdo HTML para o tooltip
            html = f"""
            <div style="font-family: Arial; font-size: 12px">
                <b>Código SGO:</b> {row['cod_sgo']}<br>
                <b>Descrição:</b> {row['descr_obra']}<br>
                <b>Tipo Obra:</b> {row['tipo_obra']}<br>
                <b>Nota SGO:</b> {row['nota_sgo']}<br>
                <b>BR:</b> {row['br']}<br>
                <b>UF:</b> {row['uf_2']}<br>
                <a href="{row['streetview_link']}" target="_blank" style="color: blue; text-decoration: underline;">
                    Abrir no Street View
                </a>
            </div>
            """
            
            iframe = folium.IFrame(html, width=250, height=150)
            popup = folium.Popup(iframe, max_width=250)
            
            folium.CircleMarker(
                location=[row['latitude'], row['longitude']],
                radius=4,
                color='black',
                fill=True,
                fill_opacity=0.5,
                popup=popup
            ).add_to(m)
        
        # Adicionar diferentes tipos de mapas base
        folium.TileLayer('OpenStreetMap', name='Rodovias').add_to(m)
        folium.TileLayer('CartoDB.Positron', name='Light Mode').add_to(m)
        folium.TileLayer('CartoDB.DarkMatter', name='Dark Mode').add_to(m)
        folium.TileLayer(
            tiles='https://mt1.google.com/vt/lyrs=s&x={x}&y={y}&z={z}',
            attr='Google',
            name='Satélite (Google)',
            max_zoom=20
        ).add_to(m)
        
        folium.LayerControl().add_to(m)
        return m

    # Sidebar para filtros
    with st.sidebar:
        st.header("Filtros")
        
        # Filtro de UF
        uf_options = ['Todos'] + sorted(df_oae['uf'].dropna().unique().tolist())
        selected_uf = st.selectbox(
            "UF",
            uf_options,
            key="uf",
            index=0 if 'uf' not in st.session_state else uf_options.index(st.session_state.uf) if st.session_state.uf in uf_options else 0
        )
        
        # Filtro de Conflitos (segundo filtro)
        temp_conflito_df = df_oae.copy()
        if selected_uf != 'Todos':
            temp_conflito_df = temp_conflito_df[temp_conflito_df['uf'] == selected_uf]
        
        conflito_options = ['Todos', 'Sim', 'Não']
        selected_conflito = st.selectbox(
            "Conflitos",
            conflito_options,
            key="conflitos",
            index=0 if 'conflitos' not in st.session_state else conflito_options.index(st.session_state.conflitos) if st.session_state.conflitos in conflito_options else 0
        )
        
        # Filtro de Tipo de Conflito (depende de Conflitos == 'Sim')
        if selected_conflito == 'Sim':
            temp_tipo_conflito_df = temp_conflito_df.copy()
            if selected_conflito != 'Todos':
                temp_tipo_conflito_df = temp_tipo_conflito_df[temp_tipo_conflito_df['conflitos'] == selected_conflito]
            
            # Filtro Tipo de Conflito
            tipo_conflito_options = ['Todos'] + sorted(temp_tipo_conflito_df['tipo_conflito'].dropna().unique().tolist())
            selected_tipo_conflito = st.selectbox(
                "Tipo de Conflito",
                tipo_conflito_options,
                key="tipo_conflito",
                index=0 if 'tipo_conflito' not in st.session_state else tipo_conflito_options.index(st.session_state.tipo_conflito) if st.session_state.tipo_conflito in tipo_conflito_options else 0
            )
        else:
            selected_tipo_conflito = None
        
    # Novo Filtro de Tipo de Administração (depende de UF, Conflitos e Tipo de Conflito)
        temp_tipo_ad_df = df_oae.copy()
        if selected_uf != 'Todos':
            temp_tipo_ad_df = temp_tipo_ad_df[temp_tipo_ad_df['uf'] == selected_uf]
        if selected_conflito != 'Todos':
            temp_tipo_ad_df = temp_tipo_ad_df[temp_tipo_ad_df['conflitos'] == selected_conflito]
        if selected_conflito == 'Sim' and selected_tipo_conflito and selected_tipo_conflito != 'Todos':
            temp_tipo_ad_df = temp_tipo_ad_df[temp_tipo_ad_df['tipo_conflito'] == selected_tipo_conflito]
        
        tipo_ad_options = ['Todos'] + sorted(temp_tipo_ad_df['ds_tipo_ad'].dropna().unique().tolist())
        selected_tipo_ad = st.selectbox(
            "Tipo de Administração",
            tipo_ad_options,
            key="ds_tipo_ad",
            index=0 if 'ds_tipo_ad' not in st.session_state else tipo_ad_options.index(st.session_state.ds_tipo_ad) if st.session_state.ds_tipo_ad in tipo_ad_options else 0
        )

    # Filtro de br (agora depende também do Tipo de Administração)
        temp_br_df = temp_tipo_ad_df.copy()
        if selected_tipo_ad != 'Todos':
            temp_br_df = temp_br_df[temp_br_df['ds_tipo_ad'] == selected_tipo_ad]
        
        br_options = ['Todos'] + sorted(temp_br_df['br'].dropna().unique().tolist())
        selected_br = st.selectbox(
            "Rodovia",
            br_options,
            key="br",
            index=0 if 'br' not in st.session_state else br_options.index(st.session_state.br) if st.session_state.br in br_options else 0
        )
        
        # Filtro de Tipo de Obra (agora depende também do Tipo de Administração)
        temp_tipo_df = temp_br_df.copy()
        if selected_br != 'Todos':
            temp_tipo_df = temp_tipo_df[temp_tipo_df['br'] == selected_br]
        
        tipo_obra_options = ['Todos'] + sorted(temp_tipo_df['tipo_obra'].dropna().unique().tolist())
        selected_tipo_obra = st.selectbox(
            "Tipo de Obra",
            tipo_obra_options,
            key="tipo_obra",
            index=0 if 'tipo_obra' not in st.session_state else tipo_obra_options.index(st.session_state.tipo_obra) if st.session_state.tipo_obra in tipo_obra_options else 0
        )
        
        # Filtro de Nota (agora depende também do Tipo de Administração)
        temp_nota_df = temp_tipo_df.copy()
        if selected_tipo_obra != 'Todos':
            temp_nota_df = temp_nota_df[temp_nota_df['tipo_obra'] == selected_tipo_obra]
        
        nota_options = ['Todos'] + sorted(temp_nota_df['nota_sgo'].dropna().astype(str).unique().tolist())
        selected_nota = st.selectbox(
            "Nota",
            nota_options,
            key="nota_sgo",
            index=0 if 'nota_sgo' not in st.session_state else nota_options.index(st.session_state.nota_sgo) if st.session_state.nota_sgo in nota_options else 0
        )

    # Aplicar filtros
    filtered_oae = df_oae.copy()
    filtered_snv = df_snv.copy()

    # Aplicar filtro de UF
    if selected_uf != 'Todos':
        filtered_oae = filtered_oae[filtered_oae['uf'] == selected_uf]
        filtered_snv = filtered_snv[filtered_snv['sg_uf'] == selected_uf]

    # Aplicar filtro de Conflitos
    if selected_conflito != 'Todos':
        filtered_oae = filtered_oae[filtered_oae['conflitos'] == selected_conflito]

    # Aplicar filtro de Tipo de Conflito (se aplicável)
    if selected_conflito == 'Sim' and selected_tipo_conflito and selected_tipo_conflito != 'Todos':
        filtered_oae = filtered_oae[filtered_oae['tipo_conflito'] == selected_tipo_conflito]

    # Aplicar filtro de Tipo de Administração
    if selected_tipo_ad != 'Todos':
        filtered_oae = filtered_oae[filtered_oae['ds_tipo_ad'] == selected_tipo_ad]
        filtered_snv = filtered_snv[filtered_snv['ds_tipo_ad'] == selected_tipo_ad]

    # Aplicar outros filtros
    if selected_br != 'Todos':
        br_selecionado = str(selected_br).zfill(3)
        filtered_oae = filtered_oae[filtered_oae['br'].astype(str).str.zfill(3) == br_selecionado]
        filtered_snv = filtered_snv[filtered_snv['vl_br'].astype(str).str.zfill(3) == br_selecionado]

    if selected_tipo_obra != 'Todos':
        filtered_oae = filtered_oae[filtered_oae['tipo_obra'] == selected_tipo_obra]
        
    if selected_nota != 'Todos':
        filtered_oae = filtered_oae[filtered_oae['nota_sgo'].astype(str) == selected_nota]
        
    # Mostrar contagem de registros
    col1, col2 = st.columns(2)
    col1.write(f"SNV visíveis: {len(filtered_snv)}")
    col2.write(f"Obras de Arte Especiais visíveis: {len(filtered_oae)}")

    # Seção Street View integrada com pesquisa inteligente
    st.subheader("Street View - Visualização por OAE")
    selected_streetview = st_searchbox(
        search_function=search_oae_with_streetview,
        placeholder="Pesquisar por código ou nome da OAE para Street View...",
        label="Pesquisa para Street View:",
        key="streetview_search",
        default=None
    )

    # Quando uma obra é selecionada
    if selected_streetview:
        # Extrai o código SGO da seleção (remove o "(Abrir Street View)" do label)
        codigo_sgo = selected_streetview.split(" - ")[0]
        obra_data = df_oae[df_oae['cod_sgo'].astype(str) == codigo_sgo].iloc[0]
        
        st.session_state.selected_obra_streetview = {
            'cod_sgo': obra_data['cod_sgo'],
            'descr_obra': obra_data['descr_obra'],
            'latitude': float(obra_data['latitude'].split(';')[0]),
            'longitude': float(obra_data['longitude'].split(';')[0])
        }

    # Mostra detalhes e botão se houver obra selecionada
    if st.session_state.selected_obra_streetview:
        obra = st.session_state.selected_obra_streetview
        st.write(f"**Obra selecionada:** {obra['cod_sgo']} - {obra['descr_obra']}")
        st.write(f"**Localização:** Latitude {obra['latitude']}, Longitude {obra['longitude']}")
        
        if st.button("Abrir no Google Street View"):
            open_street_view(obra['latitude'], obra['longitude'])

    # Criar e exibir o mapa
    if not filtered_snv.empty or not filtered_oae.empty:
        m = create_map(filtered_snv, filtered_oae, st.session_state.selected_obra_streetview)
        folium_static(m, width=1400, height=800)
    else:
        st.warning("Nenhum dado encontrado com os filtros selecionados.")
else:
    st.warning("Por favor, carregue todos os arquivos necessários para continuar.")
