# Trabajo Práctico: Introducción a Databricks y Procesamiento con Apache Spark
**Materia:** Análisis de Datos en la Nube  
**Docente:** Prof. Luis A. Chiaramonte  
**Alumnos / Integrantes:** [Completar con Nombres y Apellidos]  
**Plataforma utilizada:** Databricks Community Edition (Apache Spark 3.x / 4.x + Delta Lake)

---

## 1. Resumen del Proyecto

En este trabajo práctico implementamos un proceso ETL (Extracción, Transformación y Carga) básico de extremo a extremo utilizando **Databricks Community Edition** y **Apache Spark**. 

Para la práctica elegimos un caso real de comercio electrónico: analizamos las ventas de una tienda y las cruzamos con su catálogo de productos. A lo largo del notebook limpiamos registros corruptos y nulos, realizamos cruces de tablas (`joins`), calculamos márgenes de facturación y persistimos los datos procesados en **Delta Lake**, validando su integridad mediante consultas con **Spark SQL**.

---

## 2. Estructura del Repositorio

```text
tp-databricks-spark/
│
├── data/
│   ├── ventas.csv                 # Registro transaccional de ventas (fechas, cantidades, descuentos)
│   └── productos.csv              # Catálogo con nombres, categorías y precios unitarios
│
├── notebook_databricks_tp.ipynb   # Notebook estándar (formato Jupyter, listo para importar en Databricks)
├── notebook_databricks_source.py  # Código fuente nativo de Databricks con celdas %python, %sql y %md
└── README.md                      # Documentación y descripción del entregable
```

---

## 3. Desarrollo del Pipeline (Etapa por Etapa)

### Etapa 1: Configuración del entorno
* Creamos una cuenta gratuita en **Databricks Community Edition**.
* Levantamos un clúster estándar de prueba con un solo nodo (Driver) y creamos un notebook asociado.
* Verificamos el acceso a la sesión predeterminada de Spark (`spark.version`).

### Etapa 2: Carga y exploración de datos
* Subimos los archivos `ventas.csv` y `productos.csv` al almacenamiento de Databricks (DBFS).
* Leímos los archivos con `spark.read.format("csv")` utilizando `header=True` e inferencia automática de esquemas (`inferSchema=True`).
* Inspeccionamos las primeras filas con `display()` y revisamos los tipos de datos inferidos con `printSchema()`.

### Etapa 3: Transformaciones y limpieza (Data Cleansing)
Durante la exploración detectamos dos problemas habituales en los datos de origen:
1. **Registros duplicados:** Había transacciones repetidas cargadas dos veces por error. Las eliminamos utilizando `dropDuplicates(["id_venta"])`.
2. **Valores nulos en descuentos:** Algunas compras no tenían cargado el porcentaje de descuento. Los rellenamos con `0.0` mediante `fillna()`, asumiendo que correspondían a ventas sin promoción.
3. **Cruce de tablas (Join):** Realizamos un `inner join` entre las ventas y los productos vinculándolos por `id_producto`. De esta manera, cada venta quedó enriquecida con el nombre del producto, su categoría y su precio real de lista.
4. **Columnas calculadas:** 
   * `subtotal` = `cantidad * precio_unitario`
   * `monto_descuento` = `subtotal * descuento`
   * `total_venta` = `subtotal - monto_descuento`

### Etapa 4: Almacenamiento optimizado con Delta Lake
Guardamos el DataFrame resultante en formato **Delta Lake** (`format("delta")`). 
Elegimos Delta Lake frente al almacenamiento tradicional en CSV o Parquet por sus ventajas clave:
* **Garantía de transacciones ACID:** Asegura que no queden archivos a medio escribir si el clúster falla durante la escritura.
* **Capacidad de Time Travel:** Permite consultar versiones históricas de la tabla en caso de necesitar auditar o recuperar datos.
* **Validación de integridad:** Volvimos a leer la tabla directamente desde la ruta Delta para constatar que el esquema y la cantidad de filas persistidas fueran exactamente las mismas.

### Etapa 5: Consultas con Spark SQL y Visualización
Registramos la tabla Delta como una vista temporal (`createOrReplaceTempView("vista_ventas")`) para poder explotar el motor de **Spark SQL** con sentencias tradicionales (`%sql`):
* Calculamos la facturación acumulada y cantidad de pedidos por categoría de producto.
* Identificamos el Top 3 de productos con mayor recaudación de la tienda.
* Generamos un gráfico de barras comparativo mostrando la distribución del ingreso por categoría.

---

## 4. Conclusiones del Trabajo

1. **Eficiencia de Databricks:** La plataforma simplifica notablemente la curva de aprendizaje al eliminar la necesidad de configurar máquinas virtuales, instalar Java o administrar nodos manualmente. En pocos clics se dispone de un clúster funcional y un entorno colaborativo.
2. **Flexibilidad políglota:** Poder combinar celdas de Python con consultas nativas en SQL dentro del mismo cuaderno permite que perfiles de ingeniería y analistas de negocio trabajen sobre la misma fuente de datos sin barreras de lenguaje.
3. **Confiabilidad con Delta Lake:** En un pipeline de producción es fundamental contar con capas de calidad como Delta Lake para garantizar que las transformaciones queden blindadas ante caídas y con control de versiones.
4. **Hallazgo del análisis:** La categoría **Tecnología** concentra el mayor volumen de facturación total de la tienda (impulsada principalmente por la venta de computadoras portátiles), a pesar de que la categoría **Periféricos** registra una mayor cantidad de transacciones individuales de menor valor.

---

## 5. Cómo Importar y Ejecutar el Notebook en Databricks

1. Ingresar a [Databricks Community Edition](https://community.cloud.databricks.com/).
2. Ir a la sección **Compute** y crear un clúster (tarda unos 2 a 3 minutos en iniciar).
3. En la barra lateral, ir a **Workspace** -> **Users** -> Tu usuario.
4. Hacer clic con el botón derecho, seleccionar **Import** y subir el archivo `notebook_databricks_tp.ipynb` (o `notebook_databricks_source.py`).
5. En la sección **Catalog** / **Create Table**, subir los dos archivos de la carpeta `data/` (`ventas.csv` y `productos.csv`).
6. Asociar el clúster al notebook y ejecutar las celdas en orden con `Shift + Enter` o **Run All**.
