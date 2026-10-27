# Trabajo Práctico: Introducción a Databricks y Procesamiento con Apache Spark
**Materia:** Análisis de Datos en la Nube  
**Docente:** Prof. Luis A. Chiaramonte  
**Alumnos / Integrantes:** Melocchi Sol, Olaiz Bruno, Rojas Augusto, Vicino Lucas  
**Plataforma utilizada:** Databricks Free Edition (cómputo serverless, Unity Catalog y Delta Lake)

---

## 1. Resumen del Proyecto

En este trabajo práctico implementamos un proceso ETL (Extracción, Transformación y Carga) de extremo a extremo con **Databricks Free Edition** y **Apache Spark**.

Usamos un caso de comercio electrónico: analizamos las ventas de una tienda y las cruzamos con su catálogo de productos. En el notebook exploramos los datos con PySpark y Spark SQL, eliminamos duplicados y completamos nulos, cruzamos las tablas con un `join`, calculamos la facturación de cada venta, guardamos el resultado en **Delta Lake**, validamos su integridad, probamos el *time travel* y analizamos los resultados con consultas SQL y gráficos.

---

## 2. Estructura del Repositorio

```text
databricks-spark-lakehouse-pipeline/
│
├── data/
│   ├── ventas.csv                 # 22 registros de ventas (fecha, producto, cliente, cantidad, descuento)
│   └── productos.csv              # Catálogo de 10 productos con nombre, categoría y precio
│
├── notebook_databricks_tp.ipynb   # Notebook en formato Jupyter, listo para importar en Databricks
├── notebook_databricks_tp_resultados.html  # Notebook ejecutado en Databricks, con resultados y gráficos
├── notebook_databricks_source.py  # Mismo notebook en formato fuente de Databricks (%md, %sql)
└── README.md                      # Este documento
```

---

## 3. Desarrollo del Pipeline (Etapa por Etapa)

### Etapa 1: Configuración del entorno
* Creamos una cuenta gratuita en **Databricks Free Edition** e importamos el notebook.
* Free Edition usa cómputo **serverless**: no hace falta crear un clúster, el notebook se conecta solo al ejecutar la primera celda.
* Verificamos la sesión de Spark (`spark.version`) y creamos un volumen de Unity Catalog (`workspace.default.tp7`) donde subimos los CSV.

### Etapa 2: Carga y exploración de datos
* Leímos los archivos con `spark.read.format("csv")` usando `header=True` e `inferSchema=True`.
* Mostramos las primeras filas con `display()` y los tipos de datos con `printSchema()`.
* Registramos las tablas crudas como vistas temporales y las exploramos con **Spark SQL**: cantidad de filas, ventas duplicadas, descuentos nulos, rango de fechas y productos por categoría.

### Etapa 3: Transformaciones de datos
La exploración mostró dos problemas en las ventas:
1. **Duplicados:** las ventas `VTA_101` y `VTA_107` estaban cargadas dos veces. Las eliminamos con `dropDuplicates(["id_venta"])` (de 22 filas quedaron 20).
2. **Nulos:** tres ventas no tenían descuento. Los completamos con `0.0` mediante `fillna()`, asumiendo que fueron ventas sin promoción.

Después:
3. **Join:** cruzamos ventas y productos por `id_producto` (`inner join`) para obtener nombre, categoría y precio de cada venta.
4. **Columnas calculadas:** `subtotal = cantidad * precio_unitario`, `monto_descuento = subtotal * descuento`, `total_venta = subtotal - monto_descuento` y `mes` (año y mes de la venta).
5. **Filtro y agregación:** filtramos las ventas con descuento y agrupamos por categoría.

### Etapa 4: Almacenamiento con Delta Lake
* Guardamos el resultado como tabla Delta administrada por Unity Catalog: `workspace.default.ventas_transformadas`.
* **Validación de integridad:** volvimos a leer la tabla y comprobamos que coincidieran la cantidad de filas, la facturación total y el esquema.
* **Time travel:** simulamos un error borrando las ventas de Audio, consultamos el historial con `DESCRIBE HISTORY`, leímos la versión anterior al borrado y restauramos la tabla con `RESTORE TABLE`.

### Etapa 5: Visualización y análisis
Con Spark SQL sobre la tabla Delta calculamos:
* Facturación, operaciones y unidades por categoría, con el porcentaje de cada una.
* El top 3 de productos con mayor recaudación.
* La evolución mensual de la facturación.

Además armamos un **gráfico de barras** (facturación por categoría) y un **gráfico de líneas** (facturación por mes).

---

## 4. Resultados

El notebook se ejecutó completo en Databricks Free Edition. Todas las salidas (tablas, validaciones, historial de la tabla Delta y gráficos) se pueden ver descargando y abriendo en el navegador el archivo `notebook_databricks_tp_resultados.html`.

| Categoría | Operaciones | Unidades | Facturación | % |
|---|---|---|---|---|
| Tecnología | 7 | 13 | $6.402.500 | 72 % |
| Mobiliario | 4 | 4 | $1.449.000 | 16 % |
| Periféricos | 6 | 12 | $657.950 | 7 % |
| Audio | 3 | 4 | $411.000 | 5 % |
| **Total** | **20** | **33** | **$8.920.450** | |

| Mes | Facturación |
|---|---|
| 2024-01 | $2.143.900 |
| 2024-02 | $3.533.250 |
| 2024-03 | $3.243.300 |

---

## 5. Conclusiones

**Sobre los datos**
1. **Tecnología es la categoría principal en todos los indicadores:** tiene más ventas (7), más unidades (13) y el 72 % de la facturación. El producto que más recaudó es la Notebook Gamer 15.6 ($4.812.500 en 4 unidades).
2. **Periféricos tiene casi tantas ventas como Tecnología pero factura mucho menos** (7 %), porque son productos baratos. Mobiliario, con solo 4 ventas, factura más del doble que Periféricos.
3. **Febrero fue el mejor mes** ($3.533.250, 40 % del total) y el de más ventas (9).
4. **La limpieza cambió los resultados:** sin eliminar los 2 duplicados, la facturación habría quedado inflada en $2.437.500 (27 % más), porque los registros repetidos eran ventas de notebooks.

**Sobre la plataforma**
1. Databricks evita instalar y configurar Spark: con el cómputo serverless alcanza con abrir el notebook y ejecutar.
2. En un mismo notebook combinamos Python, SQL y texto explicativo.
3. Delta Lake protege los datos con transacciones ACID y, como vimos con el time travel, permite recuperar una tabla después de un borrado por error.

---

## 6. Cómo ejecutar el notebook en Databricks Free Edition

1. Crear una cuenta gratuita en [Databricks Free Edition](https://www.databricks.com/learn/free-edition).
2. Ir a **Workspace → Home**, hacer clic en los tres puntos (o clic derecho) → **Import** y subir `notebook_databricks_tp.ipynb` (o `notebook_databricks_source.py`).
3. Abrir el notebook y ejecutar las celdas de la Etapa 1 hasta el `CREATE VOLUME`.
4. Ir a **Catalog → workspace → default → Volumes → tp7 → Upload to this volume** y subir `ventas.csv` y `productos.csv` de la carpeta `data/`.
5. Volver al notebook y ejecutar todo con **Run all**.
