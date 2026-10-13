# Databricks notebook source
# MAGIC %md
# MAGIC # Trabajo Práctico: Introducción a Databricks y Procesamiento con Apache Spark
# MAGIC **Materia:** Análisis de Datos en la Nube  
# MAGIC **Profesor:** Luis A. Chiaramonte  
# MAGIC **Integrantes:** [Completar con Nombres de los Integrantes]  
# MAGIC 
# MAGIC ---
# MAGIC ### ¿De qué trata este trabajo?
# MAGIC El objetivo de este TP es dar nuestros primeros pasos en **Databricks Community Edition**, armando un pipeline ETL (Extracción, Transformación y Carga) completo con **Apache Spark**.
# MAGIC 
# MAGIC Vamos a:
# MAGIC 1. Levantar un cluster y un notebook en Databricks.
# MAGIC 2. Cargar dos tablas de un comercio electrónico (`ventas.csv` y `productos.csv`).
# MAGIC 3. Limpiar los datos (sacar duplicados y emprolijar nulos).
# MAGIC 4. Transformar y cruzar las tablas con un `join` para calcular métricas clave de facturación.
# MAGIC 5. Guardar la tabla limpia en **Delta Lake** y validar que se pueda leer sin problemas.
# MAGIC 6. Hacer consultas con **Spark SQL** y gráficos para sacar conclusiones del negocio.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 1: Configuración del entorno
# MAGIC En Databricks la sesión de Spark ya viene lista y corriendo en segundo plano dentro de la variable global `spark`. Importamos las funciones que vamos a necesitar a lo largo del TP.

# COMMAND ----------

from pyspark.sql.functions import col, round, sum, avg, count, when, desc
import matplotlib.pyplot as plt

print(f"Sesión de Spark activa correctamente: Versión {spark.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 2: Carga y exploración de datos
# MAGIC Cargamos los dos archivos CSV que subimos previamente a DBFS en Databricks. Le ponemos `header=True` para que use la primera fila como nombres de columna y `inferSchema=True` para que detecte automáticamente los tipos de datos.

# COMMAND ----------

# Rutas de los archivos subidos al FileStore de Databricks
path_ventas = "/FileStore/tables/ventas.csv"
path_productos = "/FileStore/tables/productos.csv"

# Leemos las ventas
df_ventas_raw = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(path_ventas)

# Leemos el catálogo de productos
df_productos_raw = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(path_productos)

print(f"Total de filas en ventas: {df_ventas_raw.count()}")
print(f"Total de productos: {df_productos_raw.count()}")

# COMMAND ----------

# Visualizamos las primeras filas con display() propio de Databricks
display(df_ventas_raw.limit(5))

# COMMAND ----------

display(df_productos_raw.limit(5))

# COMMAND ----------

# Vemos la estructura y tipos de datos que infirió Spark
df_ventas_raw.printSchema()
df_productos_raw.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 3: Transformaciones y limpieza de datos
# MAGIC En esta etapa hacemos la parte fuerte del ETL:
# MAGIC 1. **Manejo de nulos:** en `descuento` detectamos filas vacías. Asumimos que si no vino descuento, el valor es `0.0`.
# MAGIC 2. **Eliminar duplicados:** encontramos filas repetidas que cargó el sistema dos veces por error.
# MAGIC 3. **Join:** cruzamos las ventas con la tabla de productos usando `id_producto` para saber el nombre, categoría y precio de cada venta.
# MAGIC 4. **Columnas calculadas:** calculamos el subtotal (`cantidad * precio_unitario`) y el total neto aplicando el descuento.

# COMMAND ----------

# 1. Limpieza de duplicados
filas_antes = df_ventas_raw.count()
df_ventas_sin_dup = df_ventas_raw.dropDuplicates(["id_venta"])
filas_despues = df_ventas_sin_dup.count()

print(f"Filas antes: {filas_antes} | Filas después de eliminar duplicados: {filas_despues}")
print(f"Se eliminaron {filas_antes - filas_despues} registros duplicados.")

# 2. Rellenamos los descuentos nulos con 0.0
df_ventas_limpio = df_ventas_sin_dup.fillna({"descuento": 0.0})

# COMMAND ----------

# 3. Hacemos el Join con la tabla de productos por id_producto
df_completo = df_ventas_limpio.join(
    df_productos_raw,
    on="id_producto",
    how="inner"
)

# 4. Creamos columnas calculadas para el negocio
# subtotal = cantidad * precio_unitario
# total_venta = subtotal - (subtotal * descuento)
df_transformado = df_completo \
    .withColumn("subtotal", round(col("cantidad") * col("precio_unitario"), 2)) \
    .withColumn("monto_descuento", round(col("subtotal") * col("descuento"), 2)) \
    .withColumn("total_venta", round(col("subtotal") - col("monto_descuento"), 2))

display(df_transformado.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 4: Almacenamiento con Delta Lake
# MAGIC Guardamos la tabla transformada en formato **Delta Lake**. A diferencia de guardar en CSV o Parquet común, Delta nos da:
# MAGIC - **Transacciones ACID:** no se rompe la tabla si se cae el proceso a la mitad.
# MAGIC - **Time Travel:** podemos consultar versiones anteriores de la tabla.
# MAGIC - **Mayor velocidad:** gracias al registro transaccional y la indexación.
# MAGIC 
# MAGIC Guardamos los datos y después los leemos de vuelta para validar la integridad.

# COMMAND ----------

# Guardamos en formato Delta Lake (modo overwrite para poder reejecutar sin problemas)
delta_path = "/FileStore/tables/delta/ventas_transformadas"

df_transformado.write \
    .format("delta") \
    .mode("overwrite") \
    .save(delta_path)

print(f"Datos guardados exitosamente en Delta Lake en: {delta_path}")

# COMMAND ----------

# Leemos nuevamente desde Delta Lake para validar la integridad
df_delta = spark.read.format("delta").load(delta_path)

print(f"Filas leídas desde Delta: {df_delta.count()}")
print("Esquema verificado en Delta Lake:")
df_delta.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 5: Consultas con Spark SQL, Visualización y Análisis
# MAGIC Registramos el DataFrame de Delta Lake como una vista temporal (`vista_ventas`) para poder tirar consultas SQL directas y crear visualizaciones.

# COMMAND ----------

# Creamos la vista temporal para usar SQL
df_delta.createOrReplaceTempView("vista_ventas")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Consulta 1: Facturación total y cantidad de productos vendidos por Categoría
# MAGIC SELECT 
# MAGIC     categoria,
# MAGIC     COUNT(id_venta) AS operaciones,
# MAGIC     SUM(cantidad) AS unidades_vendidas,
# MAGIC     ROUND(SUM(total_venta), 2) AS facturacion_total
# MAGIC FROM vista_ventas
# MAGIC GROUP BY categoria
# MAGIC ORDER BY facturacion_total DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Consulta 2: Top 3 productos que más dinero generaron
# MAGIC SELECT 
# MAGIC     nombre_producto,
# MAGIC     categoria,
# MAGIC     SUM(cantidad) AS unidades_totales,
# MAGIC     ROUND(SUM(total_venta), 2) AS total_recaudado
# MAGIC FROM vista_ventas
# MAGIC GROUP BY nombre_producto, categoria
# MAGIC ORDER BY total_recaudado DESC
# MAGIC LIMIT 3;

# COMMAND ----------

# Visualización gráfica de ventas por categoría
pdf_cat = spark.sql("""
    SELECT categoria, ROUND(SUM(total_venta) / 1000000, 2) AS facturacion_millones
    FROM vista_ventas
    GROUP BY categoria
    ORDER BY facturacion_millones DESC
""").toPandas()

plt.figure(figsize=(9, 4.5))
bars = plt.bar(pdf_cat["categoria"], pdf_cat["facturacion_millones"], color=["#1E3A8A", "#E25822", "#0284C7", "#475569"])
plt.title("Facturación Total por Categoría (en Millones de $)", fontsize=13, fontweight="bold", pad=12)
plt.xlabel("Categoría", fontsize=11)
plt.ylabel("Millones de $", fontsize=11)
plt.grid(axis="y", linestyle="--", alpha=0.5)

for bar in bars:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2.0, yval + 0.05, f"${yval:.2f}M", ha="center", va="bottom", fontsize=9, fontweight="bold")

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 6: Conclusiones del Trabajo
# MAGIC 
# MAGIC A partir del procesamiento y análisis de los datos en Databricks, llegamos a las siguientes conclusiones:
# MAGIC 
# MAGIC 1. **Facilidad del entorno Databricks:** La plataforma resuelve toda la parte pesada de configurar máquinas y Spark. En pocos minutos tuvimos un cluster activo y pudimos combinar código Python con consultas SQL en el mismo notebook sin fricción.
# MAGIC 2. **Importancia de la etapa de limpieza en el ETL:** Encontrar y eliminar duplicados y nulos antes de hacer los cálculos evitó que inflemos la facturación o tengamos errores de cálculo con los descuentos.
# MAGIC 3. **Valor de Delta Lake:** Guardar en formato Delta nos garantiza que la tabla queda protegida con transacciones ACID, evitando tablas a medio escribir o datos corruptos ante cualquier falla del cluster.
# MAGIC 4. **Hallazgo del negocio:** La categoría **Tecnología** es por lejos el motor de ingresos de la tienda (encabezada por las ventas de Notebooks Gamer), a pesar de que la categoría **Periféricos** tiene mayor cantidad de transacciones individuales.
