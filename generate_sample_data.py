import csv

def create_sample_data():
    productos = [
        ["PROD_01", "Notebook Gamer 15.6", "Tecnologia", 1250000.0],
        ["PROD_02", "Monitor 27 Pulgadas 144Hz", "Tecnologia", 340000.0],
        ["PROD_03", "Teclado Mecanico RGB", "Perifericos", 75000.0],
        ["PROD_04", "Mouse Inalambrico Ergonomico", "Perifericos", 42000.0],
        ["PROD_05", "Auriculares Bluetooth Cancelacion Ruido", "Audio", 115000.0],
        ["PROD_06", "Silla Ergonomica de Oficina", "Mobiliario", 280000.0],
        ["PROD_07", "Escritorio Elevable Electrico", "Mobiliario", 490000.0],
        ["PROD_08", "Camara Web Full HD", "Perifericos", 58000.0],
        ["PROD_09", "Parlante Portatil Resistente al Agua", "Audio", 89000.0],
        ["PROD_10", "Disco Solido SSD NVMe 1TB", "Tecnologia", 110000.0],
    ]

    with open("data/productos.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id_producto", "nombre_producto", "categoria", "precio_unitario"])
        writer.writerows(productos)

    ventas = [
        ["VTA_101", "PROD_01", "CLI_050", "2024-01-15", 1, 0.05],
        ["VTA_102", "PROD_03", "CLI_022", "2024-01-16", 2, 0.0],
        ["VTA_103", "PROD_02", "CLI_014", "2024-01-18", 1, ""],    # Nulo a limpiar
        ["VTA_104", "PROD_04", "CLI_088", "2024-01-20", 3, 0.10],
        ["VTA_105", "PROD_05", "CLI_012", "2024-01-22", 1, 0.0],
        ["VTA_106", "PROD_06", "CLI_034", "2024-01-25", 1, 0.15],
        ["VTA_107", "PROD_01", "CLI_071", "2024-02-01", 1, 0.0],
        ["VTA_108", "PROD_10", "CLI_095", "2024-02-03", 4, 0.05],
        ["VTA_109", "PROD_07", "CLI_011", "2024-02-05", 1, ""],    # Nulo a limpiar
        ["VTA_110", "PROD_08", "CLI_063", "2024-02-08", 2, 0.0],
        ["VTA_111", "PROD_02", "CLI_040", "2024-02-12", 2, 0.10],
        ["VTA_112", "PROD_09", "CLI_019", "2024-02-14", 1, 0.0],
        ["VTA_113", "PROD_03", "CLI_081", "2024-02-18", 1, 0.05],
        ["VTA_114", "PROD_05", "CLI_055", "2024-02-22", 2, 0.10],
        ["VTA_115", "PROD_06", "CLI_027", "2024-02-26", 1, 0.0],
        ["VTA_116", "PROD_01", "CLI_003", "2024-03-02", 2, 0.05],
        ["VTA_117", "PROD_04", "CLI_044", "2024-03-05", 1, ""],    # Nulo a limpiar
        ["VTA_118", "PROD_10", "CLI_068", "2024-03-10", 2, 0.0],
        ["VTA_119", "PROD_07", "CLI_092", "2024-03-14", 1, 0.10],
        ["VTA_120", "PROD_08", "CLI_015", "2024-03-20", 3, 0.05],
        # Duplicados a propósito para mostrar la limpieza:
        ["VTA_101", "PROD_01", "CLI_050", "2024-01-15", 1, 0.05], # Duplicado
        ["VTA_107", "PROD_01", "CLI_071", "2024-02-01", 1, 0.0],  # Duplicado
    ]

    with open("data/ventas.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id_venta", "id_producto", "id_cliente", "fecha", "cantidad", "descuento"])
        writer.writerows(ventas)

    print("Datasets generados correctamente en data/ventas.csv y data/productos.csv")

if __name__ == "__main__":
    create_sample_data()
