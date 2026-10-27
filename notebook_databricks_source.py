# Databricks notebook source
# MAGIC %md
# MAGIC # Trabajo Práctico: Introducción a Databricks y Procesamiento con Apache Spark
# MAGIC **Materia:** Análisis de Datos en la Nube
# MAGIC **Profesor:** Luis A. Chiaramonte
# MAGIC **Integrantes:** Melocchi Sol, Olaiz Bruno, Rojas Augusto, Vicino Lucas
# MAGIC **Plataforma:** Databricks Free Edition (cómputo serverless + Unity Catalog + Delta Lake)
# MAGIC
# MAGIC ---
# MAGIC ### ¿De qué trata este trabajo?
# MAGIC Armamos un pipeline ETL (Extracción, Transformación y Carga) completo con **Apache Spark** en Databricks, usando dos tablas de un comercio electrónico: `ventas.csv` y `productos.csv`.
# MAGIC
# MAGIC 1. Preparamos el entorno y el almacenamiento.
# MAGIC 2. Cargamos los CSV y los exploramos con PySpark y Spark SQL.
# MAGIC 3. Limpiamos (duplicados y nulos), cruzamos las tablas con un `join` y calculamos columnas nuevas.
# MAGIC 4. Guardamos el resultado en **Delta Lake**, validamos la integridad y probamos el *time travel*.
# MAGIC 5. Analizamos con Spark SQL y gráficos de barras y de líneas.
# MAGIC 6. Sacamos conclusiones.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 1: Configuración del entorno
# MAGIC En Databricks la sesión de Spark ya viene creada en la variable `spark`. Verificamos la versión e importamos las funciones que vamos a usar.
# MAGIC
# MAGIC Importamos las funciones de Spark como `F` (por ejemplo `F.sum`) para no pisar las funciones propias de Python como `sum` o `round`.

# COMMAND ----------

import pyspark.sql.functions as F
import matplotlib.pyplot as plt

print(f"Sesión de Spark activa. Versión de Spark: {spark.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC En Databricks Free Edition los archivos se guardan en **volúmenes** de Unity Catalog. Creamos un volumen llamado `tp7` dentro del catálogo `workspace` y el esquema `default`, que vienen creados en la cuenta gratuita.
# MAGIC
# MAGIC **Después de ejecutar esta celda** hay que subir `ventas.csv` y `productos.csv` al volumen: menú **Catalog → workspace → default → Volumes → tp7 → Upload to this volume**.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.default.tp7;

# COMMAND ----------

# Ruta del volumen donde subimos los CSV
ruta_volumen = "/Volumes/workspace/default/tp7"

# Listamos el contenido para confirmar que los dos archivos están subidos
display(dbutils.fs.ls(ruta_volumen))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 2: Carga y exploración de datos
# MAGIC Leemos los dos CSV con `header=True` (la primera fila tiene los nombres de las columnas) e `inferSchema=True` (Spark detecta si cada columna es número, texto o fecha).

# COMMAND ----------

df_ventas_raw = (spark.read.format("csv")
                 .option("header", "true")
                 .option("inferSchema", "true")
                 .load(f"{ruta_volumen}/ventas.csv"))

df_productos_raw = (spark.read.format("csv")
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .load(f"{ruta_volumen}/productos.csv"))

print(f"Filas en ventas:    {df_ventas_raw.count()}")
print(f"Filas en productos: {df_productos_raw.count()}")

# COMMAND ----------

# Primeras filas de cada tabla
display(df_ventas_raw.limit(10))

# COMMAND ----------

display(df_productos_raw)

# COMMAND ----------

# Tipos de datos que infirió Spark
df_ventas_raw.printSchema()
df_productos_raw.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exploración con Spark SQL
# MAGIC Registramos las tablas crudas como vistas temporales para consultarlas con SQL y entender su contenido antes de limpiarlas.

# COMMAND ----------

df_ventas_raw.createOrReplaceTempView("ventas_raw")
df_productos_raw.createOrReplaceTempView("productos_raw")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ¿Cuántas ventas hay, cuántos id_venta distintos y cuántos descuentos vacíos?
# MAGIC SELECT
# MAGIC     COUNT(*)                                         AS filas_totales,
# MAGIC     COUNT(DISTINCT id_venta)                         AS ventas_distintas,
# MAGIC     COUNT(*) - COUNT(DISTINCT id_venta)              AS filas_duplicadas,
# MAGIC     SUM(CASE WHEN descuento IS NULL THEN 1 ELSE 0 END) AS descuentos_nulos,
# MAGIC     MIN(fecha)                                       AS primera_venta,
# MAGIC     MAX(fecha)                                       AS ultima_venta
# MAGIC FROM ventas_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ¿Qué id_venta están repetidos?
# MAGIC SELECT id_venta, COUNT(*) AS veces
# MAGIC FROM ventas_raw
# MAGIC GROUP BY id_venta
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cantidad de productos del catálogo por categoría
# MAGIC SELECT categoria, COUNT(*) AS productos, MIN(precio_unitario) AS precio_min, MAX(precio_unitario) AS precio_max
# MAGIC FROM productos_raw
# MAGIC GROUP BY categoria
# MAGIC ORDER BY productos DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 3: Transformaciones de datos
# MAGIC La exploración mostró dos problemas en las ventas:
# MAGIC 1. **Duplicados:** dos ventas (`VTA_101` y `VTA_107`) están cargadas dos veces. Las eliminamos con `dropDuplicates` sobre `id_venta`.
# MAGIC 2. **Nulos:** tres ventas no tienen cargado el `descuento`. Asumimos que fueron ventas sin promoción y los completamos con `0.0`.
# MAGIC
# MAGIC Después cruzamos las ventas con el catálogo (`join` por `id_producto`) y calculamos columnas nuevas.

# COMMAND ----------

# 1. Eliminamos duplicados
filas_antes = df_ventas_raw.count()
df_ventas_sin_dup = df_ventas_raw.dropDuplicates(["id_venta"])
filas_despues = df_ventas_sin_dup.count()
print(f"Filas antes: {filas_antes} | después: {filas_despues} | duplicados eliminados: {filas_antes - filas_despues}")

# 2. Completamos los descuentos nulos con 0.0
df_ventas_limpio = df_ventas_sin_dup.fillna({"descuento": 0.0})
nulos_restantes = df_ventas_limpio.filter(F.col("descuento").isNull()).count()
print(f"Descuentos nulos después de la limpieza: {nulos_restantes}")

# COMMAND ----------

# 3. Join: cada venta toma el nombre, la categoría y el precio del producto
df_completo = df_ventas_limpio.join(df_productos_raw, on="id_producto", how="inner")

# 4. Columnas calculadas
#    subtotal        = cantidad * precio_unitario
#    monto_descuento = subtotal * descuento
#    total_venta     = subtotal - monto_descuento
#    mes             = año y mes de la venta, para analizar la evolución en el tiempo
df_transformado = (df_completo
    .withColumn("subtotal", F.round(F.col("cantidad") * F.col("precio_unitario"), 2))
    .withColumn("monto_descuento", F.round(F.col("subtotal") * F.col("descuento"), 2))
    .withColumn("total_venta", F.round(F.col("subtotal") - F.col("monto_descuento"), 2))
    .withColumn("mes", F.date_format(F.col("fecha"), "yyyy-MM")))

# Filtro: ventas con descuento aplicado
df_con_descuento = df_transformado.filter(F.col("descuento") > 0)
print(f"Ventas totales: {df_transformado.count()} | ventas con descuento: {df_con_descuento.count()}")

display(df_transformado.select("id_venta", "fecha", "nombre_producto", "categoria", "cantidad",
                               "precio_unitario", "descuento", "subtotal", "total_venta"))

# COMMAND ----------

# 5. Agregación: totales por categoría con la API de DataFrames
display(df_transformado.groupBy("categoria")
        .agg(F.count("id_venta").alias("operaciones"),
             F.sum("cantidad").alias("unidades"),
             F.round(F.sum("total_venta"), 2).alias("facturacion"))
        .orderBy(F.desc("facturacion")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 4: Almacenamiento con Delta Lake
# MAGIC Guardamos la tabla transformada como **tabla Delta** administrada por Unity Catalog (`workspace.default.ventas_transformadas`). Delta guarda los datos en archivos Parquet y agrega un registro de transacciones que nos da:
# MAGIC - **Transacciones ACID:** si el proceso falla a mitad de la escritura, la tabla no queda a medio escribir.
# MAGIC - **Control de esquema:** rechaza datos que no coinciden con la estructura de la tabla.
# MAGIC - **Time travel:** cada cambio genera una versión nueva y se pueden consultar o restaurar versiones anteriores.

# COMMAND ----------

tabla_delta = "workspace.default.ventas_transformadas"

# Modo overwrite para poder volver a ejecutar el notebook sin errores
df_transformado.write.format("delta").mode("overwrite").saveAsTable(tabla_delta)
print(f"Tabla Delta guardada: {tabla_delta}")

# COMMAND ----------

# Leemos de nuevo desde Delta y validamos la integridad
df_delta = spark.read.table(tabla_delta)

filas_origen = df_transformado.count()
filas_delta = df_delta.count()
total_origen = df_transformado.agg(F.sum("total_venta")).first()[0]
total_delta = df_delta.agg(F.sum("total_venta")).first()[0]

print(f"Filas:       origen = {filas_origen} | Delta = {filas_delta}")
print(f"Facturación: origen = {total_origen:,.2f} | Delta = {total_delta:,.2f}")
print(f"Mismo esquema: {df_transformado.schema == df_delta.schema}")

assert filas_origen == filas_delta, "La cantidad de filas no coincide"
assert round(total_origen, 2) == round(total_delta, 2), "La facturación no coincide"
print("Validación correcta: los datos leídos desde Delta son iguales a los que se guardaron.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time travel
# MAGIC Para probar el historial de versiones simulamos un error: borramos por equivocación las ventas de la categoría Audio. Después consultamos la versión anterior y restauramos la tabla.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Error simulado: se borran las ventas de Audio (esto crea una versión nueva de la tabla)
# MAGIC DELETE FROM workspace.default.ventas_transformadas WHERE categoria = 'Audio';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Historial de la tabla: cada operación quedó registrada como una versión
# MAGIC DESCRIBE HISTORY workspace.default.ventas_transformadas;

# COMMAND ----------

# Comparamos la versión actual con la anterior al borrado
historial = spark.sql(f"DESCRIBE HISTORY {tabla_delta}").orderBy(F.desc("version")).collect()
version_actual = historial[0]["version"]
version_anterior = historial[1]["version"]

filas_ahora = spark.read.table(tabla_delta).count()
filas_antes_del_error = spark.sql(f"SELECT * FROM {tabla_delta} VERSION AS OF {version_anterior}").count()
print(f"Versión {version_actual} (después del DELETE): {filas_ahora} filas")
print(f"Versión {version_anterior} (antes del DELETE):  {filas_antes_del_error} filas")

# Restauramos la tabla a la versión anterior al error
spark.sql(f"RESTORE TABLE {tabla_delta} TO VERSION AS OF {version_anterior}")
print(f"Tabla restaurada. Filas actuales: {spark.read.table(tabla_delta).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 5: Visualización y análisis
# MAGIC Consultamos la tabla Delta con Spark SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Consulta 1: facturación, operaciones y unidades por categoría
# MAGIC SELECT
# MAGIC     categoria,
# MAGIC     COUNT(id_venta)            AS operaciones,
# MAGIC     SUM(cantidad)              AS unidades_vendidas,
# MAGIC     ROUND(SUM(total_venta), 2) AS facturacion_total,
# MAGIC     ROUND(100 * SUM(total_venta) / SUM(SUM(total_venta)) OVER (), 1) AS porcentaje
# MAGIC FROM workspace.default.ventas_transformadas
# MAGIC GROUP BY categoria
# MAGIC ORDER BY facturacion_total DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Consulta 2: top 3 productos que más dinero generaron
# MAGIC SELECT
# MAGIC     nombre_producto,
# MAGIC     categoria,
# MAGIC     SUM(cantidad)              AS unidades,
# MAGIC     ROUND(SUM(total_venta), 2) AS total_recaudado
# MAGIC FROM workspace.default.ventas_transformadas
# MAGIC GROUP BY nombre_producto, categoria
# MAGIC ORDER BY total_recaudado DESC
# MAGIC LIMIT 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Consulta 3: evolución mensual de la facturación
# MAGIC SELECT mes, COUNT(id_venta) AS operaciones, ROUND(SUM(total_venta), 2) AS facturacion
# MAGIC FROM workspace.default.ventas_transformadas
# MAGIC GROUP BY mes
# MAGIC ORDER BY mes;

# COMMAND ----------

# Gráfico de barras: facturación por categoría
pdf_cat = spark.sql(f"""
    SELECT categoria, SUM(total_venta) / 1e6 AS millones
    FROM {tabla_delta}
    GROUP BY categoria
    ORDER BY millones DESC
""").toPandas()

plt.figure(figsize=(9, 4.5))
barras = plt.bar(pdf_cat["categoria"], pdf_cat["millones"], color="#1E3A8A")
plt.title("Facturación total por categoría", fontsize=13, fontweight="bold")
plt.xlabel("Categoría")
plt.ylabel("Millones de $")
plt.grid(axis="y", linestyle="--", alpha=0.5)
for b in barras:
    plt.text(b.get_x() + b.get_width() / 2, b.get_height() + 0.05, f"${b.get_height():.2f}M",
             ha="center", va="bottom", fontsize=9, fontweight="bold")
plt.tight_layout()
plt.show()

# COMMAND ----------

# Gráfico de líneas: evolución mensual de la facturación
pdf_mes = spark.sql(f"""
    SELECT mes, SUM(total_venta) / 1e6 AS millones
    FROM {tabla_delta}
    GROUP BY mes
    ORDER BY mes
""").toPandas()

plt.figure(figsize=(9, 4.5))
plt.plot(pdf_mes["mes"], pdf_mes["millones"], marker="o", linewidth=2, color="#E25822")
plt.title("Evolución mensual de la facturación", fontsize=13, fontweight="bold")
plt.xlabel("Mes")
plt.ylabel("Millones de $")
plt.grid(linestyle="--", alpha=0.5)
for x, y in zip(pdf_mes["mes"], pdf_mes["millones"]):
    plt.annotate(f"${y:.2f}M", (x, y), textcoords="offset points", xytext=(0, 8), ha="center", fontsize=9)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 6: Conclusiones
# MAGIC
# MAGIC **Sobre los datos**
# MAGIC 1. **Tecnología es la categoría principal en todos los indicadores:** concentra 7 de las 20 ventas, 13 de las 33 unidades y $6.402.500 de los $8.920.450 facturados (72 %). El producto que más recaudó es la Notebook Gamer 15.6 ($4.812.500 en 4 unidades).
# MAGIC 2. **Periféricos tiene casi tantas ventas como Tecnología (6) pero factura mucho menos** ($657.950, 7 %), porque son productos de bajo precio. Mobiliario, con solo 4 ventas, factura más del doble que Periféricos ($1.449.000).
# MAGIC 3. **Febrero fue el mejor mes** ($3.533.250, 40 % del total) y también el de más ventas (9). Marzo quedó cerca ($3.243.300) gracias a una sola venta de dos notebooks.
# MAGIC 4. **La limpieza cambió los resultados:** sin eliminar los 2 duplicados, la facturación habría quedado inflada en $2.437.500 (27 % más), porque los dos registros repetidos eran ventas de notebooks.
# MAGIC
# MAGIC **Sobre la plataforma**
# MAGIC 1. Databricks evita instalar y configurar Spark: con el cómputo serverless de Free Edition alcanza con abrir el notebook y ejecutar.
# MAGIC 2. En un mismo notebook combinamos Python, SQL y texto explicativo.
# MAGIC 3. Delta Lake protege los datos con transacciones ACID y, como vimos con el time travel, permite recuperar la tabla después de un borrado por error.
