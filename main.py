"""
Alumnos: Gonzalo Caporaletti, Victoria Carbonel, Micaela Gonzalez Dardik

LABORATORIO de DATOS: intensivo de verano

Tema: Trabajo Práctico 01. Manejo y visualización de Datos
Fecha de entrega: 23 de febrero de 2025 
"""

#%% PROCESAMIENTO Y LIMPIEZA DE DATOS

#%%% IMPORTS Y CARGA DE DATOS
import pandas as pd
import numpy as np
import duckdb as dd
import re
import unicodedata
import matplotlib.pyplot as plt
import seaborn as sns

#%% FUNCIONES

def reemplazar_si_empieza(texto):  # Cambia los nombres de 'Comuna X' a 'ciudad autonoma de buenos aires'
    if isinstance(texto, str):  # Verifica que es un string
        if texto.startswith("comuna"):
            return "ciudad autonoma de buenos aires"  # Nuevo valor
    return texto  # Deja el valor original si no coincide


def quitar_tildes(texto):  # Saca las tildes y convierte a minúsculas
    if isinstance(texto, str):  # Verifica que sea texto
        return ''.join(
            c for c in unicodedata.normalize('NFD', texto)
            if unicodedata.category(c) != 'Mn'
        ).lower()  # Convierte a minúsculas 
    return texto

# %% Cargamos las Bases de Datos de cada fuente

carpeta = "TablasOriginales\\"#carpeta donde se encuentran las tablas originales

CC = pd.read_csv(carpeta+"centros_culturales.csv")

ee = pd.read_excel(carpeta+"2022_padron_oficial_establecimientos_educativos.xlsX", skiprows=6)
# Saltea las primeras 6 líneas correspondientes a títulos y encabezados

pp = pd.read_excel(carpeta+"padron_poblacion.xlsX", skiprows=12)
# Saltea las primeras 12 líneas correspondientes a títulos y encabezados

#%% LIMPIEZA DE DATOS
#%% BASE de DATOS: CC

CC = CC.replace({0: 's/d', '-': 's/d'})  # Reemplaza 0 y '-' en todas las columnas
CC = CC.fillna('s/d')  # Reemplaza los valores nulos (NaN)
CC = CC.reset_index()  # Convierto el índice en una columna para identificar los CC
CC.rename(columns={'index': 'ID_CC'}, inplace=True)  # Lo renombro
CC.rename(columns={'Mail ': 'Mail'}, inplace=True)  # Elimino el espacio en el nombre de la columna Mail

'---------------------------------corrijo celdas desplazadas--------------------------------------------'

conn = dd.connect(database='CC.duckdb', read_only=False)  # Creo un archivo de la base de datos para usarlo como base en duckdb
conn.execute("DROP TABLE IF EXISTS CC")  # Eliminar la tabla CC si existe (para evitar conflictos si se ejecuta más de una vez)
conn.execute("CREATE TABLE CC AS SELECT * FROM CC")  # Creo la tabla base CC a partir del DataFrame en el archivo

consultaSQL = """
                    UPDATE CC
                    SET 
                    Domicilio = 'Falucho 1418',
                    Piso = '',              
                    CP = '',  
                    cod_area = '11',
                    Telefóno = '44400547',
                    Mail = 'Ignaciodevedia@gmail.com',
                    Web = '',               
                    InfoAdicional = '',
                    Latitud = '-34.38956050',
                    Longitud = '-58.73757020',
                    TipoLatitudLongitud = 'Precisa',
                    Fuente = 'Puntos de Cultura 2020',           
                    año_inicio = '',
                    Capacidad = '',      
                    Actualizacion = '2020'     
                    WHERE Nombre = 'Casa Popular Marielle Franco';
"""
conn.execute(consultaSQL)

consultaSQL = """
                    UPDATE CC
                    SET 
                    Latitud = '-45.81641540',
                    Longitud = '-67.45550320',
                    TipoLatitudLongitud = 'Precisa',
                    Fuente = 'Puntos de Cultura 2020',           
                    Capacidad = 0 ,      
                    Actualizacion = '2020'     
                    WHERE Nombre = 'Espacio Cultural "Kultural 5"';
"""
conn.execute(consultaSQL)

CC = conn.execute("SELECT * FROM CC").df()

# Elimino las columnas que no usaremos y Paso Departamento a Minúscula
consultaSQL = '''SELECT ID_CC, 
                 ID_PROV, ID_DEPTO, 
                 Provincia, 
                 lower(Departamento) AS Departamento, 
                 Nombre,
                 Mail, 
                 Capacidad 
                 FROM CC'''
       
CC = dd.sql(consultaSQL).df()

CC["Departamento"] = CC["Departamento"].apply(quitar_tildes)  # Elimino las tildes

CC['Departamento'] = CC['Departamento'].replace({  # Cambio los nombres de los departamentos que no coincidan con el df de Departamentos
    'grl. jose de san martin': 'general jose de san martin',
    "san nicolas de los arroyos": "san nicolas",
    "o' higgins": "o'higgins",
    'primero de mayo': "1º de mayo",
})

#%% BASE de DATOS: ee

ee["Departamento"] = ee["Departamento"].apply(quitar_tildes)

ee = ee.rename(columns={"Jurisdicción": "Provincia"})

'''Arreglo a la base de datos ee'''

# Cambio los departamentos que empiezan con comuna a ciudad autonoma de buenos aires
ee["Departamento"] = ee["Departamento"].apply(reemplazar_si_empieza) 

# Reemplazo lugares vacíos por NaN
ee = ee.replace(' ', np.nan)  # Convierte espacios en blanco en NaN
ee = ee.replace('', np.nan)  # Convierte strings vacíos en NaN

# Reemplazo NaNs por 0
ee = ee.fillna(0)  # Rellena NaN con s/d

# Modifico los 0 por s/d en las columnas que corresponde
columnas_a_modificar = ['Provincia', 'Cueanexo', 'Nombre', 'Sector', 'Ámbito', 'Domicilio', 'C. P.', 'Código de área', 'Teléfono', 'Código de localidad', 'Localidad', 'Departamento', 'Mail']
ee[columnas_a_modificar] = ee[columnas_a_modificar].replace({0: 's/d', '-': 's/d'})

# Filtro las filas donde la columna 'Común' tenga un valor de 1
ee = ee[ee["Común"].astype(int) == 1]  # 1 era un string, forzosamente lo transformo en un int

# Cambio específico de algunos departamentos que no coinciden con la entidad departamento
ee['Departamento'] = ee['Departamento'].replace({
    '1§ de mayo': '1º de mayo',
    'coronel de marina l rosales': 'coronel de marina leonardo rosales',
    'coronel felipe varela': 'general felipe varela',
    'o higgins': "o'higgins",
    'doctor manuel belgrano': "dr. manuel belgrano",
    'mayor luis j fontana': "mayor luis j. fontana",
    'general juan f quiroga': 'general juan facundo quiroga',
    'general ocampo': 'general ortiz de ocampo',
    'juan f ibarra': 'juan felipe ibarra',
})

# Cambio específico de algunas provincias para que coincidan con la entidad provincia
ee['Provincia'] = ee['Provincia'].replace({
    'Ciudad de Buenos Aires': 'Ciudad Autónoma de Buenos Aires',
    'Tierra del Fuego': 'Tierra del Fuego, Antártida e Islas del Atlántico Sur',
})

# Selecciono solo las columnas que necesito para trabajar y analizar
columnas_necesarias = [
    "Provincia", "Cueanexo", "Nombre", "Departamento",
    "Nivel inicial - Jardín maternal", "Nivel inicial - Jardín de infantes",
    "Primario", "Secundario", "Secundario - INET", "SNU", "SNU - INET"
]
ee = ee[columnas_necesarias]


# %% BASE de DATOS: Población

# Renombrar columnas
pp = pp.rename(
    columns={
        "Unnamed: 0": "descartar",
        "Unnamed: 1": "Edad",
        "Unnamed: 2": "Poblacion",
        "Unnamed: 3": "%",
        "Unnamed: 4": "Acumulado %"
    }
)

# Eliminar filas completamente vacías
pp = pp.dropna(how="all")

# Quitar filas que tengan 'Total' en 'Edad'
pp = pp[pp['Edad'] != 'Total']

# Crear columnas "Área" y "Comuna" para detectar "AREA #..."
pp["Área"] = None
pp["Comuna"] = None

area_actual = None
comuna_actual = None

# Iterar sobre las filas para detectar "AREA #..." y asociarlas
for i, row in pp.iterrows():
    valor_edad = row["Edad"]
    if isinstance(valor_edad, str) and "AREA #" in valor_edad:
        area_actual = valor_edad
        comuna_actual = row["Poblacion"]
        # Se anula la columna "Edad" en esa fila (era un encabezado)
        pp.at[i, "Edad"] = None
    
    pp.at[i, "Área"]   = area_actual
    pp.at[i, "Comuna"] = comuna_actual

# Extraer codigo_provincia y codigo_depto de la cadena “AREA #...”
pp['codigo_provincia'] = pp['Área'].str[7:9]  # p.ej. "02"
pp['codigo_depto']     = pp['Área'].str[9:]   # p.ej. "007"


# Eliminar filas a partir de la palabra 'RESUMEN'
indice_resumen = pp[pp["Edad"] == "RESUMEN"].index
if not indice_resumen.empty:
    primer_resumen = indice_resumen.min()
    pp = pp.loc[:primer_resumen - 1]

# Eliminar filas que tengan NaN en la columna '%'
pp = pp.dropna(subset=["%"])

# Eliminar filas que todavía tengan 'Edad' en la columna Edad (eran encabezados)
pp = pp[pp['Edad'] != 'Edad']

# Eliminar la columna 'descartar' si existe
pp = pp.drop(columns=['descartar'], errors='ignore')

# Unificar comunas de CABA
pp["Comuna"] = pp["Comuna"].astype(str)
pp["Comuna"] = np.where(
    pp["Comuna"].str.startswith("Comuna"), 
    "ciudad autonoma de buenos aires", 
    pp["Comuna"]
)
pp.rename(columns={"Comuna": "Departamento"}, inplace=True)

# Agrupar filas de CABA (provincia "02") en un solo Departamento
mascara_caba = (pp["codigo_provincia"] == "02")
pp_caba = (
    pp[mascara_caba]
    .groupby("Edad", as_index=False)
    .agg({
        "Poblacion": "sum",
        "%": "sum",
        "Acumulado %": "sum"
    })
)

pp_caba["codigo_provincia"] = "02"
pp_caba["codigo_depto"]     = "000"
pp_caba["Departamento"]     = "ciudad autonoma de buenos aires"


pp_caba["Área"] = "AREA # 02000"


pp_sin_caba = pp[~mascara_caba]
pp = pd.concat([pp_caba, pp_sin_caba], ignore_index=True)

pp["Departamento"] = pp["Departamento"].apply(quitar_tildes)



consultaSQL = "SELECT DISTINCT ID_PROV, Provincia FROM CC ORDER BY ID_PROV"
Provincias = dd.sql(consultaSQL).df()
Provincias['ID_PROV'] = Provincias['ID_PROV'].astype(int)

#construyo las tablas departamentos
consultaSQL = '''
SELECT DISTINCT 
    Área,
    codigo_provincia,
    codigo_depto,
    Departamento
FROM pp
ORDER BY Departamento
'''
Departamentos = dd.sql(consultaSQL).df()

# Agregar departamentos faltantes ya con la provincia asignada
nuevos_departamentos = pd.DataFrame({
    "Área": [None, None, None, None, None],  # Si no tienen Área real, van como None
    "codigo_provincia": [6, 6, 6, 82, 94],   # Códigos de provincia correctos
    "codigo_depto": [np.nan, np.nan, np.nan, np.nan, np.nan],
    "Departamento": [
        "pigue", 
        "veronica", 
        "coronel brandsen", 
        "santa fe", 
        "antartida argentina"
    ]
})

Departamentos = pd.concat([Departamentos, nuevos_departamentos], ignore_index=True)


#  Eliminar la columna 'ID_DEPTO' si existe, para reindexar
Departamentos.drop(columns=['ID_DEPTO'], inplace=True, errors='ignore')

# Asignar un ID_DEPTO consecutivo
Departamentos['ID_DEPTO'] = range(1, len(Departamentos) + 1)

#  Convertir codigo_provincia a entero
Departamentos['codigo_provincia'] = (
    Departamentos['codigo_provincia'].fillna(-1).astype(int)
)

# ordenar
Departamentos = Departamentos.sort_values("ID_DEPTO")


#  Para que CABA aparezca, verifica que su fila ahora existe
#         con Área = None, codigo_provincia=2, codigo_depto=000, etc.
#         Asignar la misma clave en 'pp'

#  Construir un diccionario { Área : ID_DEPTO }
dict_area_to_id = dict(zip(Departamentos['Área'], Departamentos['ID_DEPTO']))


pp['ID_DEPTO'] = pp['Área'].map(dict_area_to_id)


#  Dejar la tabla "Departamentos" SÓLO con ID_DEPTO, Departamento e ID_PROV

Departamentos['ID_PROV'] = Departamentos['codigo_provincia']
Departamentos.drop(columns=['codigo_provincia','codigo_depto','Área'], inplace=True, errors='ignore')

# Reordenar columnas
Departamentos = Departamentos[['ID_DEPTO','Departamento','ID_PROV']]


#Crear df_final agrupando (Edad, ID_DEPTO) en pp, si lo deseas

df_final = (
    pp.groupby(["Edad", "ID_DEPTO"], as_index=False)
      .agg({"Poblacion": "sum"})
)

print("Departamentos (final):\n", Departamentos.head(20))
print("df_final (poblacion):\n", df_final.head(20))


#%%
CC['ID_DEPTO'] = CC['ID_DEPTO'].astype(int)
Departamentos['ID_DEPTO'] = Departamentos['ID_DEPTO'].astype(int) 

CC['ID_PROV'] = CC['ID_PROV'].astype(int)
Departamentos['ID_PROV'] = Departamentos['ID_PROV'].astype(int)                # Convertir a int las columnas

CC.drop(columns=['ID_DEPTO'], inplace=True)                                    # Eliminar la antigua columna de ID_DEPTO
CC = CC.merge(Departamentos, on=['Departamento', 'ID_PROV'], how='left')       # Asignar el ID_DEPTO correcto a cada fila
# %%
ee = ee.merge(Provincias, on= 'Provincia', how='left')
ee = ee.merge(Departamentos, on=['Departamento', 'ID_PROV'], how='left')       # Agregar código de departamento

#%% 
# Armamos las bases a partir del DER que planteamos mediante funciones de Pandas y consultas SQL

# Consulta a la tabla de establecimientos educativos
consultaSQL = """ 
                SELECT DISTINCT Cueanexo,
                Nombre, 
                ID_DEPTO
                FROM ee;
               """
Establecimientos_E = dd.sql(consultaSQL).df()

# Consulta a la tabla de centros culturales
consultaSQL = ''' 
                  SELECT DISTINCT ID_CC,
                          CC.Nombre, 
                          CC.Capacidad,
                          CC.ID_DEPTO
                  FROM CC
                  ORDER BY Nombre; 
                '''
Centros_C = dd.sql(consultaSQL).df()

# Consulta para crear la tabla de niveles educativos
consultaSQL = """
                  SELECT 1 AS id_Nivel_Educativo, 'Nivel inicial - Jardín Maternal' AS Nombre UNION ALL
                  SELECT 2, 'Nivel inicial - Jardín de Infantes' UNION ALL
                  SELECT 3, 'Primario' UNION ALL
                  SELECT 4, 'Secundario' UNION ALL
                  SELECT 5, 'Secundario - INET' UNION ALL
                  SELECT 6, 'SNU' UNION ALL
                  SELECT 7, 'SNU - INET';
               """
Nivel_Educativo = dd.sql(consultaSQL).df()

# Consulta para unir establecimientos educativos con niveles educativos
consultaSQL = """
                    SELECT DISTINCT cueanexo, id_Nivel_Educativo
                    FROM ee AS e
                    INNER JOIN Nivel_Educativo AS m 
                    ON (e."Nivel inicial - Jardín Maternal" = '1' AND m.Nombre = 'Nivel inicial - Jardín Maternal') OR
                       (e."Nivel inicial - Jardín de Infantes" = '1' AND m.Nombre = 'Nivel inicial - Jardín de Infantes') OR
                       (e."Primario" = '1' AND m.Nombre = 'Primario') OR
                       (e."Secundario" = '1' AND m.Nombre = 'Secundario') OR
                       (e."Secundario - INET" = '1' AND m.Nombre = 'Secundario - INET') OR
                       (e."SNU" = '1' AND m.Nombre = 'SNU') OR
                       (e."SNU - INET" = '1' AND m.Nombre = 'SNU - INET')
                   ORDER BY cueanexo;
              """
Nivel_Educativo_de_ee = dd.sql(consultaSQL).df()

# Consulta para el reporte demográfico
consultaSQL = ''' 
                  SELECT Edad, ID_DEPTO, Poblacion FROM df_final
               '''
Reporte_Demografico = dd.sql(consultaSQL).df()

#----------------------------------------- Creo la entidad débil Mails_CC -----------------------------
Mails = CC.copy()
Mails["Mail"] = Mails["Mail"].fillna("").astype(str)  # Me aseguro que la columna de mails es string y saco nans
email_pattern = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"  # Formato generico para extraer mails completos
Mails["mails_extraidos"] = Mails["Mail"].apply(lambda x: re.findall(email_pattern, x))  # Extraigo correos
Mails = Mails.explode("mails_extraidos", ignore_index=True)  # Exploto las listas en filas separadas
Mails = Mails.drop(columns=["Mail"]).rename(columns={"mails_extraidos": "Mail"})  # Renombro la columna
Mails = Mails.drop(columns=['ID_PROV', 'ID_DEPTO', 'Provincia', 'Departamento', 'Nombre', 'Capacidad'])  # Elimino atributos innecesarios

Departamentos['Departamento'] = Departamentos['Departamento'].str.title()

#%% Exporto los DataFrames
Centros_C.to_csv('Centros_Cs.csv', index=False)
Establecimientos_E.to_csv('Establecimientos_E.csv', index=False)
Nivel_Educativo.to_csv('Nivel_Educativo.csv', index=False)
Nivel_Educativo_de_ee.to_csv('Nivel_Educativo_de_ee.csv', index=False)
Departamentos.to_csv('Departamentos.csv', index=False)
Provincias.to_csv('Provincias.csv', index=False)
Mails.to_csv('Mails.csv', index=False)
Reporte_Demografico.to_csv('Reporte_Demografico.csv', index=False)

#%% EJERCICIOS DE CONSULTAS SQL
#%%% EJERCICIO 1

# Consulto Establecimientos Educativos por Nivel en cada departamento
consultaSQL = """
                WITH Escuelas AS (
                    SELECT 
                        d.ID_DEPTO,
                        SUM(CASE WHEN ne.id_Nivel_Educativo = 1 THEN 1 ELSE 0 END) AS Cant_Escuelas_Inicial,
                        SUM(CASE WHEN ne.id_Nivel_Educativo = 2 THEN 1 ELSE 0 END) AS Cant_Escuelas_Primaria,
                        SUM(CASE WHEN ne.id_Nivel_Educativo = 3 THEN 1 ELSE 0 END) AS Cant_Escuelas_Secundaria
                    FROM Departamentos AS d
                    LEFT JOIN Establecimientos_E AS ee ON d.ID_DEPTO = ee.ID_DEPTO
                    LEFT JOIN Nivel_Educativo_de_ee AS ne ON ee.Cueanexo = ne.Cueanexo
                    GROUP BY d.ID_DEPTO
                ),
                Poblacion AS (
                    SELECT 
                        ID_DEPTO,
                        SUM(CASE WHEN Edad BETWEEN 3 AND 5 THEN Poblacion ELSE 0 END) AS Poblacion_Inicial,
                        SUM(CASE WHEN Edad BETWEEN 6 AND 12 THEN Poblacion ELSE 0 END) AS Poblacion_Primaria,
                        SUM(CASE WHEN Edad BETWEEN 13 AND 18 THEN Poblacion ELSE 0 END) AS Poblacion_Secundaria
                    FROM Reporte_Demografico
                    GROUP BY ID_DEPTO
                )
                SELECT 
                    p.Provincia,
                    d.Departamento,
                    e.Cant_Escuelas_Inicial AS Jardines,
                    po.Poblacion_Inicial AS "Población Jardin",
                    e.Cant_Escuelas_Primaria AS Primarias,
                    po.Poblacion_Primaria AS "Población Primaria",
                    e.Cant_Escuelas_Secundaria AS Secundarios,
                    po.Poblacion_Secundaria AS "Población Secundaria"
                FROM Departamentos AS d
                JOIN Provincias AS p ON d.ID_PROV = p.ID_PROV
                JOIN Escuelas AS e ON d.ID_DEPTO = e.ID_DEPTO
                JOIN Poblacion AS po ON d.ID_DEPTO = po.ID_DEPTO
                ORDER BY p.Provincia ASC, e.Cant_Escuelas_Primaria DESC
              """
Nivel_Ed_por_Prov = dd.sql(consultaSQL).df()

# %%
Nivel_Ed_por_Prov.to_csv('Nivel_Ed_por_Prov.csv', index=False)


#%%% EJERCICIO 2

# Decisión: los CC con capacidad 's/d' no los contamos como mayor a 100
consultaSQL = """
                SELECT 
                    d.ID_DEPTO,
                    d.Departamento,
                    p.Provincia,
                    COUNT(CASE 
                              WHEN cc.Capacidad != 's/d' AND CAST(cc.Capacidad AS INTEGER) > 100 THEN cc.ID_CC 
                              ELSE NULL 
                          END) AS Cantidad_CC
                FROM 
                    Departamentos d
                JOIN 
                    Provincias p ON d.ID_PROV = p.ID_PROV
                LEFT JOIN 
                    Centros_C cc ON d.ID_DEPTO = cc.ID_DEPTO
                GROUP BY 
                    d.ID_DEPTO, d.Departamento, p.Provincia
                ORDER BY 
                    p.Provincia ASC,
                    Cantidad_CC DESC
              """
depto_CC_100 = dd.sql(consultaSQL).df()

# Consulta para mostrar los resultados
consultaSQL = ''' 
                 SELECT 
                 Departamento, 
                 Provincia,
                 Cantidad_CC AS "Cantidad de CC con cap>100"
                 FROM 
                 depto_CC_100
               '''
depto_CC_100 = dd.sql(consultaSQL).df()
depto_CC_100.to_csv('depto_CC_100.csv', index=False)



#%%% EJERCICIO 3

consultaSQL = """
              SELECT 
                  d.ID_DEPTO,
                  d.Departamento,
                  p.Provincia,
                  (
                      SELECT COUNT(*) 
                      FROM Centros_C cc 
                      WHERE cc.ID_DEPTO = d.ID_DEPTO
                      ) AS Cantidad_CC,
                  (
                      SELECT COUNT(*) 
                      FROM Establecimientos_E ee 
                      WHERE ee.ID_DEPTO = d.ID_DEPTO
                      ) AS Cantidad_EE,
                  (
                      SELECT SUM(Poblacion_por_Edad) 
                      FROM (
                          SELECT rd.Edad, MAX(Poblacion) AS Poblacion_por_Edad
                          FROM Reporte_Demografico rd 
                          WHERE rd.ID_DEPTO = d.ID_DEPTO
                          GROUP BY rd.Edad
                      ) AS sub_rd
                      ) AS Poblacion_Total
              FROM 
                  Departamentos d
              JOIN 
                  Provincias p ON d.ID_PROV = p.ID_PROV
              ORDER BY 
                  Cantidad_EE DESC,
                  Cantidad_CC DESC,
                  p.Provincia ASC,
                  d.Departamento ASC;

              """
Cant_CC_EE_Pob = dd.sql(consultaSQL).df()
Cant_CC_EE_Pob.to_csv('Cant_CC_EE_Pob.csv', index=False)


#%%% EJERCICIO 4

## DECISION 1: los deptos sin centro cultural no van a aparecer en este DF
## DECISION 2: los centros culturales sin mail no se consideran con dominio
### entonces si hay un depto que ninguno de sus centros tiene mail, no va a
### ser considerado para esta tabla

consulta_dominios = """
       WITH conteo_dominios AS (
           SELECT 
               d.ID_DEPTO,
               p.Provincia,
               d.Departamento,
               LOWER(SPLIT_PART(SPLIT_PART(m.Mail, '@', 2), '.', 1)) AS dominio,  ---indico cual es el dominio
               COUNT(DISTINCT cc.ID_CC) AS cnt
           FROM Departamentos d
           JOIN Provincias p ON d.ID_PROV = p.ID_PROV
           JOIN Centros_C cc ON d.ID_DEPTO = cc.ID_DEPTO
           JOIN Mails m ON cc.ID_CC = m.ID_CC
           WHERE m.Mail IS NOT NULL
           AND m.Mail <> ''                                              ---se excluyen registros que tengan cadenas vacías
           GROUP BY d.ID_DEPTO, p.Provincia, d.Departamento, dominio
           ),
       max_counts AS (
           SELECT 
               ID_DEPTO,
               MAX(cnt) AS max_cnt
           FROM conteo_dominios
           GROUP BY ID_DEPTO
           )
       SELECT 
           dc.Provincia,
           dc.Departamento,
           dc.dominio AS Dominio_mas_frecuente
       FROM conteo_dominios dc
       JOIN max_counts mc
       ON dc.ID_DEPTO = mc.ID_DEPTO AND dc.cnt = mc.max_cnt
       ORDER BY dc.Provincia ASC, dc.Departamento ASC
"""

dominios_cc = dd.sql(consulta_dominios).df()
dominios_cc.to_csv('dominios_cc.csv', index=False)



#%% VIZUALIZACIÓN DE DATOS
#%%% EJERCICIO 1

#cant de CC por provincia
consultaSQL = """
              SELECT p.Provincia, COUNT(cc.ID_CC) AS Cantidad_CC
              FROM Provincias AS p
              LEFT JOIN Departamentos AS d ON p.ID_PROV = d.ID_PROV
              LEFT JOIN Centros_C AS cc ON d.ID_DEPTO = cc.ID_DEPTO
              GROUP BY p.Provincia
              ORDER BY Cantidad_CC DESC
              """
cc_por_provincia = dd.sql(consultaSQL).df()

#cambio los nombres a tierra del fuego y caba para que el gráfico sea más legible
cc_por_provincia['Provincia'] = cc_por_provincia['Provincia'].replace({
    'Tierra del Fuego, Antártida e Islas del Atlántico Sur':'Tierra del Fuego',
    'Ciudad Autónoma de Buenos Aires':'CABA'
})

plt.figure(figsize=(10, 6))
plt.rcParams.update({'font.size': 11})
sns.barplot(data=cc_por_provincia, x='Cantidad_CC', y='Provincia', palette='viridis')
plt.title('Cantidad de Centros Culturales por Provincia')
plt.xlabel('Cantidad de Centros Culturales')
plt.ylabel('Provincia')
plt.show()


#%%% EJERCICIO 2
# Cambio el nombre de Tierra del Fuego, Antártida e Islas del Atlántico Sur para que los graficos sea más legible 
Nivel_Ed_por_Prov['Provincia'] = Nivel_Ed_por_Prov['Provincia'].replace(
    'Tierra del Fuego, Antártida e Islas del Atlántico Sur', 
    'Tierra del Fuego'
)
# Elimino la ciudad autonoma de buenos aires ya que al estar representada con un único departamento no puede ser representada con un boxplot (esta información se encuentra disponible en la primera consulta de sql)
df_filtrado = Nivel_Ed_por_Prov[Nivel_Ed_por_Prov['Provincia'] != 'Ciudad Autónoma de Buenos Aires']

# Calculo el total de las provincias y su orden para ponerlas de mayor a menos según cantidad de escuelas
df_totales = df_filtrado.groupby('Provincia')[['Jardines', 'Primarias', 'Secundarios']].sum()
df_totales['Total_EE'] = df_totales.sum(axis=1)
order_provincias = df_totales.sort_values('Total_EE', ascending=False).index.tolist()

# Transformo a formato largo para graficar
df_long = df_filtrado.melt(
    id_vars=['Provincia', 'Departamento'],
    value_vars=['Jardines', 'Primarias', 'Secundarios'],
    var_name='Grupo_Etario',
    value_name='Cantidad_EE'
)


# Grafico los boxplot
plt.figure(figsize=(35,10))
plt.rcParams.update({'font.size': 25})
sns.boxplot(
    data=df_long, 
    x='Provincia', 
    y='Cantidad_EE', 
    hue='Grupo_Etario', 
    palette='Set2',
    order=order_provincias
)
plt.xticks(rotation=90)
plt.xlabel('Provincia')
plt.ylabel('Cantidad de EE')
plt.title('Distribución de Establecimientos Educativos por Provincia y Grupo Etario')
plt.legend(title='Grupo Etario')
plt.show()


#%%% EJERCICIO 3

consultaSQL = '''SELECT ID_DEPTO, 
                 COUNT(*) AS Cantidad_de_EE 
                 FROM Establecimientos_E
                 GROUP BY ID_DEPTO 
                 ORDER BY Cantidad_de_EE''' 

Cantidad_de_EE = dd.sql(consultaSQL).df()                                                  #agrupo las escuelas por departamento para ver la cantidad en cada depto

Cantidad_de_EE = Cantidad_de_EE.merge(Departamentos, on='ID_DEPTO', how='left')            #agrego ID_PROV para poder agruparlos en provincias
Cantidad_de_EE.drop(columns=['Departamento'], inplace=True)                                #elimino el nombre del departamento
Cantidad_de_EE = Cantidad_de_EE.merge(Provincias, on='ID_PROV', how='left')               #agrego el nombre de las provincias
# Cambio el nombre de Tierra del Fuego, Antártida e Islas del Atlántico Sur para que los graficos sea más legible 
Cantidad_de_EE['Provincia'] = Cantidad_de_EE['Provincia'].replace(
    'Tierra del Fuego, Antártida e Islas del Atlántico Sur', 
    'Tierra del Fuego'
)
# Elimino la ciudad autonoma de buenos aires ya que al estar representada con un único departamento no puede ser representada con un boxplot
df_filtrado2 = Cantidad_de_EE[Cantidad_de_EE['Provincia'] != 'Ciudad Autónoma de Buenos Aires']

medianas = df_filtrado2.groupby("Provincia")["Cantidad_de_EE"].median().sort_values()    #creo un df con las medianas de la cantidad de dapartamentos por provincia
orden_provincias = medianas.index.tolist()

# grafico
plt.figure(figsize=(20,12))
sns.boxplot(x="Provincia", y="Cantidad_de_EE", data=df_filtrado2, order=orden_provincias, palette="viridis")
plt.xticks(rotation=90)
plt.xlabel("Provincia")
plt.ylabel("Cantidad de EE por Departamento")
plt.title("Distribución de EE por Departamento en cada Provincia")
plt.show()


#%% EJERCICIO 4 

# 1. Población por provincia
pop_depto = Reporte_Demografico.groupby("ID_DEPTO")["Poblacion"].sum().reset_index()                            # Sumo la población por departamento 
pop_depto = pop_depto.merge(Departamentos[["ID_DEPTO", "ID_PROV"]], on="ID_DEPTO", how="left")
pop_prov = pop_depto.groupby("ID_PROV")["Poblacion"].sum().reset_index()                                        # ahora la sumo por provincia
pop_prov = pop_prov.merge(Provincias, on="ID_PROV", how="left")                                                 # Agrego el nombre de la provincia

# 2. Establecimientos Educativos por provincia 
EE_prov = Cantidad_de_EE

# 3. Centros Culturales por provincia
CC_depto = Centros_C.merge(Departamentos[["ID_DEPTO", "ID_PROV"]], on="ID_DEPTO", how="left")
CC_prov = CC_depto.groupby("ID_PROV")["ID_CC"].count().reset_index().rename(columns={"ID_CC": "Cantidad_CC"})  # cuento los CC por provincia
CC_prov = CC_prov.merge(Provincias, on="ID_PROV", how="left")                                                   # Agrego el nombre de la provincia

pop_prov = pop_prov.replace('Tierra del Fuego, Antártida e Islas del Atlántico Sur', 'Tierra del Fuego' )
CC_prov = CC_prov.replace('Tierra del Fuego, Antártida e Islas del Atlántico Sur', 'Tierra del Fuego' )

# 4. Calcular indicadores por mil habitantes a nivel provincial
df_EE = EE_prov.merge(pop_prov, on="Provincia", how="left")                                                     # Uno la información de EE y población (usando el nombre de provincia)
df_EE["EE_por_1000"] = df_EE["Cantidad_de_EE"] / df_EE["Poblacion"] * 1000

# Hago lo mismo para CC
df_CC = CC_prov.merge(pop_prov, on="Provincia", how="left")
df_CC["CC_por_1000"] = df_CC["Cantidad_CC"] / df_CC["Poblacion"] * 1000

# Uno ambos indicadores en un solo DataFrame
df_viz = df_EE.merge(df_CC[["Provincia", "CC_por_1000"]], on="Provincia", how="outer")

# 5. Preparo los datos para el gráfico por provincia (dos barras por provincia)
df_long = pd.melt(df_viz, id_vars="Provincia", 
                  value_vars=["EE_por_1000", "CC_por_1000"],
                  var_name="Tipo", value_name="Valor")
# cambio los nombres
df_long["Tipo"] = df_long["Tipo"].map({
    "EE_por_1000": "Establecimientos Educativos",
    "CC_por_1000": "Centros Culturales"
})

# cambio los nombres a tierra del fuego y caba para que el gráfico sea más legible
df_long['Provincia'] = df_long['Provincia'].replace({
    'Tierra del Fuego, Antártida e Islas del Atlántico Sur':'Tierra del Fuego',
    'Ciudad Autónoma de Buenos Aires':'CABA'
})

# Gráfico por provincia
plt.figure(figsize=(25,15))
sns.barplot(data=df_long, x="Provincia", y="Valor", hue="Tipo", palette="Set2")
plt.xticks(rotation=90)
plt.xlabel("Provincia")
plt.ylabel("Cantidad por mil habitantes")
plt.yscale("log")
plt.title("Centros Culturales vs Establecimientos Educativos por mil habitantes por Provincia")
plt.grid(True)
plt.show()

# 6. Calculo los indicadores a nivel nacional
total_EE = EE_prov["Cantidad_de_EE"].sum()
total_CC = CC_prov["Cantidad_CC"].sum()
total_pop = pop_prov["Poblacion"].sum()

EE_por_1000_nacional = total_EE / total_pop * 1000
CC_por_1000_nacional = total_CC / total_pop * 1000

df_nacional = pd.DataFrame({
    "Tipo": ["Establecimientos Educativos", "Centros Culturales"],
    "Valor": [EE_por_1000_nacional, CC_por_1000_nacional]
})

# Gráfico nacional
plt.figure(figsize=(20,15))
sns.barplot(data=df_nacional, x="Tipo", y="Valor", palette="Set2")
plt.xlabel("")
plt.ylabel("Cantidad por mil habitantes")
plt.title("Comparación Nacional: Centros Culturales vs Establecimientos Educativos por mil habitantes")
plt.yscale('log')
plt.grid()
plt.show()
