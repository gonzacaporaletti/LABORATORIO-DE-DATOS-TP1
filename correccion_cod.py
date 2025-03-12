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

def reemplazar_si_empieza(texto):                      # Cambia los nombres de 'Comuna X' a 'ciudad autonoma de buenos aires'
    if isinstance(texto, str):                         # Verifica que es un string
        if texto.startswith("comuna"):
            return "ciudad autonoma de buenos aires"   # Nuevo valor
    return texto                                       # Deja el valor original si no coincide


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

# Creo un diccionario con las columnas a actualizar y sus nuevos valores
cambios = {
    'Domicilio': 'Falucho 1418',
    'Piso': '',
    'CP': '',
    'cod_area': '11',
    'Telefóno': '44400547',
    'Mail': 'Ignaciodevedia@gmail.com',
    'Web': '',
    'InfoAdicional': '',
    'Latitud': '-34.38956050',
    'Longitud': '-58.73757020',
    'TipoLatitudLongitud': 'Precisa',
    'Fuente': 'Puntos de Cultura 2020',
    'año_inicio': '',
    'Capacidad': '',
    'Actualizacion': '2020'
}

# Aplico la actualización solo para 'Casa Popular Marielle Franco'
mask = CC['Nombre'] == 'Casa Popular Marielle Franco'
CC.loc[mask, list(cambios.keys())] = list(cambios.values())

# repito el proceso para la siguiente fila desplazada
cambios = {
    'Latitud': '-45.81641540',
    'Longitud': '-67.45550320',
    'TipoLatitudLongitud': 'Precisa',
    'Fuente': 'Puntos de Cultura 2020',
    'Capacidad': 0,
    'Actualizacion': '2020'
}

mask = CC['Nombre'] == 'Espacio Cultural "Kultural 5"'
CC.loc[mask, list(cambios.keys())] = list(cambios.values())

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

# Renombro las columnas
pp = pp.rename(
    columns={
        "Unnamed: 0": "descartar",
        "Unnamed: 1": "Edad",
        "Unnamed: 2": "Poblacion",
        "Unnamed: 3": "%",
        "Unnamed: 4": "Acumulado %"
    }
)

# Elimino las filas completamente vacías
pp = pp.dropna(how="all")

# saco las filas que tengan 'Total' en 'Edad'
pp = pp[pp['Edad'] != 'Total']

# Creo las columnas "Área" y "Comuna" para detectar "AREA #..."
pp["Área"] = None
pp["Comuna"] = None

area_actual = None
comuna_actual = None

# Itero sobre las filas para detectar "AREA #..." y asociarlas
for i, row in pp.iterrows():
    valor_edad = row["Edad"]
    if isinstance(valor_edad, str) and "AREA #" in valor_edad:
        area_actual = valor_edad
        comuna_actual = row["Poblacion"]
        # Se anula la columna "Edad" en esa fila (era un encabezado)
        pp.loc[i, "Edad"] = None
    
    pp.loc[i, "Área"]   = area_actual
    pp.loc[i, "Comuna"] = comuna_actual

# Extraigo codigo_provincia y codigo_depto de la cadena “AREA #...”
pp['codigo_provincia'] = pp['Área'].str[7:9]
pp['codigo_depto']     = pp['Área'].str[9:]


# Elimino todas las filas del 'RESUMEN'
indice_resumen = pp[pp["Edad"] == "RESUMEN"].index
pp = pp.loc[:indice_resumen[0] - 1]

# Elimino las filas que tengan NaN en la columna '%'
pp = pp.dropna(subset=["%"])

# Elimino las filas que todavía tengan 'Edad' en la columna Edad (son encabezados)
pp = pp[pp['Edad'] != 'Edad']

# Elimino la columna 'descartar'
pp = pp.drop(columns=['descartar'])

# Unificar comunas de CABA
pp["Comuna"] = pp["Comuna"].astype(str)
pp["Comuna"] = np.where(
    pp["Comuna"].str.startswith("Comuna"),   #devuelve una Serie booleana en la que cada elemento es True si el contenido de esa celda comienza con la cadena "Comuna" y False en caso contrario.
    "ciudad autonoma de buenos aires",       #valor que se asignará cuando la condición sea True.
    pp["Comuna"]                             #si no se cumple la condición se mantiene el valor original
)
pp.rename(columns={"Comuna": "Departamento"}, inplace=True)

# Agrupo las filas de CABA en un solo Departamento
mascara_caba = (pp["codigo_provincia"] == "02")
pp_caba = (
    pp[mascara_caba]
    .groupby("Edad", as_index=False)
    .agg({                             #agg agarra todos los valores distintos de edad y suma los valores de las columnas especificadas
        "Poblacion": "sum",
        "%": "sum",
        "Acumulado %": "sum"
    })
)

pp_caba["codigo_provincia"] = "02"
pp_caba["codigo_depto"]     = "000"
pp_caba["Departamento"]     = "ciudad autonoma de buenos aires"
pp_caba["Área"]             = "AREA # 02000"


pp_sin_caba = pp[~mascara_caba]                            #el ~ invierte la condición, es decir nos devuelve todas los registros que no corresponden a caba
pp = pd.concat([pp_caba, pp_sin_caba], ignore_index=True)  #agrupo ambos df

pp["Departamento"] = pp["Departamento"].apply(quitar_tildes)

#%% CREACIÓN DE LA ENTIDAD DEPARTAMENTOS Y PROVINCIAS

#tabla Provincias
consultaSQL = '''SELECT DISTINCT ID_PROV, Provincia 
                 FROM CC 
                 ORDER BY ID_PROV'''
                
Provincias = dd.sql(consultaSQL).df()
Provincias['ID_PROV'] = Provincias['ID_PROV'].astype(int) #me aseguro de que todos sean int para evitar erroes

#tabla Departamentos
consultaSQL = '''SELECT DISTINCT 
                 Área,
                 codigo_provincia,
                 codigo_depto,
                 Departamento
                 FROM pp
                 ORDER BY Departamento'''
Departamentos = dd.sql(consultaSQL).df()

# Agrego departamentos faltantes ya con la provincia asignada
nuevos_departamentos = pd.DataFrame({
    "Área": [None, None, None, None, None],                      # lo agrego sin area porque despues se elimina la columna
    "codigo_provincia": [6, 6, 6, 82, 94],                       # Códigos de provincia correctos
    "codigo_depto": [np.nan, np.nan, np.nan, np.nan, np.nan],    # lo agrego sin código depto porque despues redefino el índice
    "Departamento": [
        "pigue", 
        "veronica", 
        "coronel brandsen", 
        "santa fe", 
        "antartida argentina"
    ]
})

Departamentos = pd.concat([Departamentos, nuevos_departamentos], ignore_index=True)


#  Elimino la columna 'ID_DEPTO', para reindexar
Departamentos.drop(columns=['ID_DEPTO'], inplace=True, errors='ignore')

# Asigno un ID_DEPTO consecutivo
Departamentos['ID_DEPTO'] = range(1, len(Departamentos) + 1)

#  Convierto codigo_provincia a int 
Departamentos['codigo_provincia'] = (
    Departamentos['codigo_provincia'].astype(int)
)

# Ordeno la tabla por "ID_DEPTO"
Departamentos = Departamentos.sort_values("ID_DEPTO")


#  Construyo un diccionario { Área : ID_DEPTO } para poder asignar el id depto correcto a pp
dict_area_to_id = dict(zip(Departamentos['Área'], Departamentos['ID_DEPTO']))


pp['ID_DEPTO'] = pp['Área'].map(dict_area_to_id) #map toma cada valor de la columna 'Área' de pp y lo busca como clave en el diccionario dict_area_to_id, de esta forma se cambia el id depto de pp por el que tiene Departamentos


# Limpio la tabla Departamentos (me quedo con las columnas que me interesan)

Departamentos['ID_PROV'] = Departamentos['codigo_provincia']
Departamentos.drop(columns=['codigo_provincia','codigo_depto','Área'], inplace=True, errors='ignore')

# Reordeno las columnas
Departamentos = Departamentos[['ID_DEPTO','Departamento','ID_PROV']]


#Creo df_final agrupando (Edad, ID_DEPTO) en pp
pp = (
    pp.groupby(["Edad", "ID_DEPTO"], as_index=False)
      .agg({"Poblacion": "sum"})
)


#%%
CC['ID_DEPTO'] = CC['ID_DEPTO'].astype(int)
Departamentos['ID_DEPTO'] = Departamentos['ID_DEPTO'].astype(int) 

CC['ID_PROV'] = CC['ID_PROV'].astype(int)
Departamentos['ID_PROV'] = Departamentos['ID_PROV'].astype(int)                # Convierto a int las columnas

CC.drop(columns=['ID_DEPTO'], inplace=True)                                    # Elimino la antigua columna de ID_DEPTO
CC = CC.merge(Departamentos, on=['Departamento', 'ID_PROV'], how='left')       # Asigno el ID_DEPTO correcto a cada fila
# %%
ee = ee.merge(Provincias, on= 'Provincia', how='left')
ee = ee.merge(Departamentos, on=['Departamento', 'ID_PROV'], how='left')       # Agrego código de departamento

#%% 
# Armamos las bases a partir del DER que planteamos mediante funciones de Pandas y consultas SQL

# Consulta a la tabla de establecimientos educativos
consultaSQL = """ 
                SELECT DISTINCT Cueanexo,
                Nombre, 
                ID_DEPTO
                FROM ee
               """
Establecimientos_E = dd.sql(consultaSQL).df()

# Consulta a la tabla de centros culturales
consultaSQL = ''' 
                  SELECT DISTINCT ID_CC,
                  CC.Nombre, 
                  CC.Capacidad,
                  CC.ID_DEPTO
                  FROM CC
                  ORDER BY Nombre 
                '''
Centros_C = dd.sql(consultaSQL).df()

# Consulta para crear la tabla de niveles educativos
consultaSQL = """
                  SELECT
                  id AS id_Nivel_Educativo,
                  CASE id
                  WHEN 1 THEN 'Nivel inicial - Jardín Maternal'
                  WHEN 2 THEN 'Nivel inicial - Jardín de Infantes'
                  WHEN 3 THEN 'Primario'
                  WHEN 4 THEN 'Secundario'
                  WHEN 5 THEN 'Secundario - INET'
                  WHEN 6 THEN 'SNU'
                  WHEN 7 THEN 'SNU - INET'
                  END AS Nombre
                  FROM range(1, 8) AS t(id);     ---se usa t() para definir una tabla temporal
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
                  SELECT Edad, ID_DEPTO, Poblacion FROM pp
               '''
Reporte_Demografico = dd.sql(consultaSQL).df()

#----------------------------------------- Creo la entidad débil Mails_CC -----------------------------
Mails = CC.copy()
Mails["Mail"] = Mails["Mail"].fillna("").astype(str)                                                     # Me aseguro que la columna de mails es string y saco nans
email_pattern = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"                                        # Formato generico para extraer mails completos
Mails["mails_extraidos"] = Mails["Mail"].apply(lambda x: re.findall(email_pattern, x))                   # Extraigo correos
Mails = Mails.explode("mails_extraidos", ignore_index=True)                                              # Exploto las listas en filas separadas
Mails = Mails.drop(columns=["Mail"]).rename(columns={"mails_extraidos": "Mail"})                         # Renombro la columna
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

# Paso 1: Genero el DataFrame de escuelas agregadas utilizando subconsultas
consultaSQL = """
                SELECT 
                d.ID_DEPTO,
                (SELECT COUNT(*) 
                 FROM Establecimientos_E AS ee 
                 JOIN Nivel_Educativo_de_ee AS ne 
                 ON ee.Cueanexo = ne.Cueanexo
                 WHERE ee.ID_DEPTO = d.ID_DEPTO 
                 AND (ne.id_Nivel_Educativo = 1 
                      OR ne.id_Nivel_Educativo = 2 )) AS Cant_Escuelas_Inicial,
                (SELECT COUNT(*) 
                 FROM Establecimientos_E AS ee 
                 JOIN Nivel_Educativo_de_ee AS ne 
                 ON ee.Cueanexo = ne.Cueanexo
                 WHERE ee.ID_DEPTO = d.ID_DEPTO 
                 AND ne.id_Nivel_Educativo = 3) AS Cant_Escuelas_Primaria,
                (SELECT COUNT(*) 
                 FROM Establecimientos_E AS ee 
                 JOIN Nivel_Educativo_de_ee AS ne 
                 ON ee.Cueanexo = ne.Cueanexo
                 WHERE ee.ID_DEPTO = d.ID_DEPTO 
                 AND (ne.id_Nivel_Educativo = 4
                      OR ne.id_Nivel_Educativo = 5)) AS Cant_Escuelas_Secundaria
                FROM Departamentos AS d
                """
df_escuelas_agregadas = dd.sql(consultaSQL).df()

# Paso 2: Genero el DataFrame de población por departamento diferenciando los rangos de edades
consultaSQL = """
                 SELECT 
                 ID_DEPTO,
                 SUM(CASE WHEN Edad BETWEEN 3 AND 5 THEN Poblacion ELSE 0 END) AS Poblacion_Inicial,
                 SUM(CASE WHEN Edad BETWEEN 6 AND 12 THEN Poblacion ELSE 0 END) AS Poblacion_Primaria,
                 SUM(CASE WHEN Edad BETWEEN 13 AND 18 THEN Poblacion ELSE 0 END) AS Poblacion_Secundaria
                 FROM Reporte_Demografico
                 GROUP BY ID_DEPTO
                 """
df_poblacion_agregada = dd.sql(consultaSQL).df()

# Paso 3: Genero el DataFrame con la información geográfica (Departamentos y Provincias)
consultaSQL = """
                 SELECT 
                 d.ID_DEPTO,
                 p.Provincia,
                 d.Departamento
                 FROM Departamentos AS d
                 JOIN Provincias AS p 
                 ON d.ID_PROV = p.ID_PROV
                 """
df_dep_prov = dd.sql(consultaSQL).df()

# Paso 4: Uno df_dep_prov con df_escuelas_agregadas para agregar los conteos de escuelas
consultaSQL = """
                 SELECT 
                 dp.Provincia,
                 dp.Departamento,
                 dp.ID_DEPTO,
                 ea.Cant_Escuelas_Inicial AS Jardines,
                 ea.Cant_Escuelas_Primaria AS Primarias,
                 ea.Cant_Escuelas_Secundaria AS Secundarios
                 FROM df_dep_prov AS dp
                 JOIN df_escuelas_agregadas AS ea
                 ON dp.ID_DEPTO = ea.ID_DEPTO
                 """
df_escuelas_dep = dd.sql(consultaSQL).df()

# Paso 5: Uno df_escuelas_dep con df_poblacion_agregada para obtener el resultado final
consultaSQL = """
                 SELECT 
                 ed.Provincia,
                 ed.Departamento,
                 ed.Jardines,
                 pa.Poblacion_Inicial AS "Población Jardin",
                 ed.Primarias,
                 pa.Poblacion_Primaria AS "Población Primaria",
                 ed.Secundarios,
                 pa.Poblacion_Secundaria AS "Población Secundaria"
                 FROM df_escuelas_dep AS ed
                 JOIN df_poblacion_agregada AS pa
                 ON ed.ID_DEPTO = pa.ID_DEPTO
                 ORDER BY ed.Provincia ASC, ed.Primarias DESC
                 """
                 
Nivel_Ed_por_Prov = dd.sql(consultaSQL).df()


# %%
Nivel_Ed_por_Prov.to_csv('Nivel_Ed_por_Prov.csv', index=False)


#%%% EJERCICIO 2

# Decisión: los CC con capacidad 's/d' no los contamos como mayor a 100

consultaSQL= ''' SELECT 
                 ID_CC,
                 ID_DEPTO,
                 ID_PROV,
                 CASE WHEN Capacidad = 's/d' 
                 THEN '0'
                 ELSE Capacidad END AS Capacidad
                 FROM CC '''

df_cambio_cap=dd.sql(consultaSQL).df()


consultaSQL='''
                SELECT d.Departamento,
                d.ID_DEPTO,
                d.ID_PROV,
                cc.Capacidad
                FROM df_cambio_cap AS cc
                RIGHT OUTER JOIN
                Departamentos AS d
                ON d.ID_DEPTO = cc.ID_DEPTO
                GROUP BY d.ID_DEPTO, d.ID_PROV
'''

depto_CC_100 = dd.sql(consultaSQL).df()



consultaSQL = ''' 
                 SELECT  
                 ID_PROV,
                 Departamento,
                 COUNT(CASE WHEN CAST(Capacidad AS INTEGER) > 100 THEN Capacidad ELSE Null END) AS "Cantidad de CC con cap>100"       --- se uitiliza CAST(Capacidad AS INTEGER) porque algunos de los registros de capacidad no son int y debemos hacerlos int, sería como usar un .astype(int)
                 FROM 
                 df_cambio_cap
                 GROUP BY Departamento, ID_PROV
                 '''
depto_CC_100 = dd.sql(consultaSQL).df()

#ahora agrego los departamentos que no están en la tabla de centros culturales


consultaSQL= '''
                SELECT 
                d.Departamento,
                d.ID_PROV
                FROM Departamentos AS d
                EXCEPT
                SELECT 
                cc.Departamento,
                cc.ID_PROV
                FROM depto_CC_100 AS cc
'''

df_todos_los_deptos=dd.sql(consultaSQL).df()

consultaSQL= '''
                SELECT
'''
#%%
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

#ahora selecciono solo que necesito

consultaSQL=''' 
                SELECT 
                Provincia,
                Departamento,
                Cantidad_EE AS "Cantidad EE",
                Cantidad_CC AS "Cantidad CC",
                Poblacion_Total AS "Población Total"
                FROM Cant_CC_EE_Pob
                '''
Cant_CC_EE_Pob = dd.sql(consultaSQL).df()

#%%
Cant_CC_EE_Pob.to_csv('Cant_CC_EE_Pob.csv', index=False)


#%%% EJERCICIO 4

## DECISION 1: los deptos sin centro cultural no van a aparecer en este DF
## DECISION 2: los centros culturales sin mail no se consideran con dominio
### entonces si hay un depto que ninguno de sus centros tiene mail, no va a
### ser considerado para esta tabla

# Paso 1: Uno Departamentos y Provincias
consulta_dp = """
        SELECT 
         d.ID_DEPTO,
         p.Provincia,
         d.Departamento
         FROM Departamentos AS d
         JOIN Provincias AS p ON d.ID_PROV = p.ID_PROV
"""
tabla_dp = dd.sql(consulta_dp).df()


# Paso 2: Un0 Centros_C y Mails para extraer el dominio
consulta_cm = """
         SELECT 
         cc.ID_DEPTO,
         LOWER(SPLIT_PART(SPLIT_PART(m.Mail, '@', 2), '.', 1)) AS dominio,
         cc.ID_CC
         FROM Centros_C AS cc
         JOIN Mails AS m ON cc.ID_CC = m.ID_CC
         WHERE m.Mail IS NOT NULL 
         AND m.Mail <> ''
"""
tabla_cm = dd.sql(consulta_cm).df()


# Paso 3: Calculo el conteo de dominios por departamento
consulta_conteo = """
         SELECT 
         ID_DEPTO,
         dominio,
         COUNT(DISTINCT ID_CC) AS cnt
         FROM tabla_cm
         GROUP BY ID_DEPTO, dominio
"""
conteo_dominios = dd.sql(consulta_conteo).df()


# Paso 4: Obtengo el máximo conteo por departamento
consulta_max = """
         SELECT 
         ID_DEPTO,
         MAX(cnt) AS max_cnt
         FROM conteo_dominios
         GROUP BY ID_DEPTO
"""
tabla_max = dd.sql(consulta_max).df()


# Paso 5: Filtro el/los dominio(s) con mayor frecuencia
consulta_max_dominio = """
         SELECT 
         cd.ID_DEPTO,
         cd.dominio,
         cd.cnt
         FROM conteo_dominios AS cd
         JOIN tabla_max AS tm ON cd.ID_DEPTO = tm.ID_DEPTO 
         AND cd.cnt = tm.max_cnt
"""
tabla_max_dominio = dd.sql(consulta_max_dominio).df()


# Paso 6: Uno con la información de Departamentos y Provincias para la tabla final
consulta_final = """
         SELECT 
         dp.Provincia,
         dp.Departamento,
         tmd.dominio AS Dominio_mas_frecuente
         FROM tabla_max_dominio AS tmd
         JOIN tabla_dp AS dp ON tmd.ID_DEPTO = dp.ID_DEPTO
         ORDER BY dp.Provincia ASC, dp.Departamento ASC
"""
dominios_cc = dd.sql(consulta_final).df()


#%%
dominios_cc.to_csv('dominios_cc.csv', index=False)

#%%
dominio_max = dominios_cc['Dominio_mas_frecuente'].value_counts().idxmax()
apariciones_dom_max = dominios_cc['Dominio_mas_frecuente'].value_counts().max()
porcentaje = (apariciones_dom_max / len(dominios_cc)) * 100
 
print(f'El dominio que más aparece es {dominio_max} con {apariciones_dom_max} apariciones, esto es el {porcentaje:.2f}% del total')
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
plt.xlabel('Cantidad de Centros Culturales')
plt.ylabel('Provincia')
plt.show()


#%%% EJERCICIO 2

# Construyo un DataFrame "apilado" para el scatterplot.

df_scatter_jardines = pd.DataFrame({
    'Poblacion': Nivel_Ed_por_Prov['Población Jardin'],
    'Cantidad_EE': Nivel_Ed_por_Prov['Jardines'],
    'Grupo_Etario': 'Jardines'
})

df_scatter_primarias = pd.DataFrame({
    'Poblacion': Nivel_Ed_por_Prov['Población Primaria'],
    'Cantidad_EE': Nivel_Ed_por_Prov['Primarias'],
    'Grupo_Etario': 'Primarias'
})

df_scatter_secundarios = pd.DataFrame({
    'Poblacion': Nivel_Ed_por_Prov['Población Secundaria'],
    'Cantidad_EE': Nivel_Ed_por_Prov['Secundarios'],
    'Grupo_Etario': 'Secundarios'
})

# Uno todo en un solo DataFrame
df_scatter = pd.concat([df_scatter_jardines, 
                        df_scatter_primarias, 
                        df_scatter_secundarios],
                       ignore_index=True)

# Grafico el scatterplot
plt.figure(figsize=(12, 8))
plt.rcParams.update({'font.size':18})
sns.scatterplot(
    data=df_scatter,
    x='Poblacion',
    y='Cantidad_EE',
    hue='Grupo_Etario',
    alpha=0.6,
    palette='Set1'
)

plt.xlabel('Población por Nivel Educativo')
plt.ylabel('Cantidad de Establecimientos Educativos')
plt.xlim(0, 90000)
plt.ylim(0, 400)
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
plt.rcParams.update({'font.size':18})
sns.boxplot(x="Provincia", y="Cantidad_de_EE", data=df_filtrado2, order=orden_provincias, palette="viridis")
plt.xticks(rotation=90)
plt.xlabel("Provincia")
plt.ylabel("Cantidad de EE por Departamento")
plt.show()


#%% EJERCICIO 4 : Scatter de Cantidad de CC cada 1000 habs en función de cantidad de EE cada 1000 habs

'''Se definen 3 clases con el fin de separar en grupos de estudio:
# CABA,
# Departamento de la Provincia de Buenos Aires,
# Departamento de otra provincia en general'''

# Se conservan del dataframe aquellos cuya provincia sea CABA
caba = Cant_CC_EE_Pob[Cant_CC_EE_Pob['Provincia'] == 'Ciudad Autónoma de Buenos Aires']
# Se conservan del dataframe aquellos cuya provincia sea Buenos Aires
buenos_aires = Cant_CC_EE_Pob[Cant_CC_EE_Pob['Provincia'] == 'Buenos Aires']
# Se conservan del dataframe aquellos cuya provincia no sea CABA ...
otras_provincias = Cant_CC_EE_Pob[Cant_CC_EE_Pob['Provincia'] != 'Ciudad Autónoma de Buenos Aires']
# ... Ni Buenos Aires
otras_provincias = otras_provincias[otras_provincias['Provincia'] != 'Buenos Aires']

# Se crean listas para almacenar los datos de los ejes x e y,
# para las tres clases
x_caba, y_caba = [], []
x_buenos_aires, y_buenos_aires = [], []
x_otras_provincias, y_otras_provincias = [], []

# Se recorren los departamentos de cada clase
# y se agregan los valores de 'Estab_x_mil' y 'CC_x_mil'
for idx, row in caba.iterrows():
    x_caba.append(row['Estab_x_mil'])
    y_caba.append(row['CC_x_mil'])

for idx, row in buenos_aires.iterrows():
    x_buenos_aires.append(row['Estab_x_mil'])
    y_buenos_aires.append(row['CC_x_mil'])

for idx, row in otras_provincias.iterrows():
    x_otras_provincias.append(row['Estab_x_mil'])
    y_otras_provincias.append(row['CC_x_mil'])

# Se crea el Scatter
plt.figure(figsize=(12, 9))

# Se grafica cada clase con diferentes colores y formas
# Se aumenta el tamaño del ítem que corresponde a CABA
plt.scatter(x_caba, y_caba, color='green', label='CABA', alpha=1, marker='o', s=300)
# Se baja la opacidad de los departamentos de Buenos Aires y de otras provincias
plt.scatter(x_buenos_aires, y_buenos_aires, color='blue', label='Departamentos de Buenos Aires', alpha=0.6, marker='^',s=70)
plt.scatter(x_otras_provincias, y_otras_provincias, color='red', label='Departamentos de otras provincias', alpha=0.6, marker='s', s=70)


# Se añaden las etiquetas y título
plt.xlabel('EE cada mil habs.')
plt.ylabel('CC cada mil habs.')

# Se añade la leyenda que especifica la clave para cada dato
plt.legend(loc='upper right', fontsize=15, title='Categorías')
plt.grid(False)


# Se presenta el gráfico
plt.show()


