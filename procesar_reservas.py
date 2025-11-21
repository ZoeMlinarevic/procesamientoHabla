"""
procesar_reservas.py

Módulo para:
- Cargar los CSV de reservas naturales de la Provincia de Buenos Aires.
- Unificar columnas y limpiar datos.
- Exportar una tabla unificada lista para usar en n8n (CSV + JSON).

Uso rápido desde terminal (dentro de la carpeta del proyecto):
    python procesar_reservas.py

Requisitos:
    pip install pandas
"""

import os
import json
import re
from typing import Dict, List

import pandas as pd
import glob


# --------------------------------------------------------------------
# 1. Configuración de archivos y categorías
# --------------------------------------------------------------------

# Mapear cada CSV a una categoría "lógica" de reserva
CSV_CONFIG: Dict[str, Dict[str, str]] = {
    "040_reserva-natural-de-objetivo-definido.-total-provincia.-2022.csv": {
        "categoria": "reserva-natural-objetivo-definido",
    },
    "041_reserva-natural-de-uso-multiple.-total-provincia.-2022.csv": {
        "categoria": "reserva-natural-uso-multiple",
    },
    "042_reserva-natural-integral.-total-provincia.-2022.csv": {
        "categoria": "reserva-natural-integral",
    },
    "043_reserva-de-biosfera.-total-provincia.-2022.csv": {
        "categoria": "reserva-de-biosfera",
    },
    "044_reserva-natural-privada.-total-provincia.-2022.csv": {
        "categoria": "reserva-natural-privada",
    },
    "045_reserva-municipal.-total-provincia.-2022.csv": {
        "categoria": "reserva-municipal",
    },
    "046_reserva-natural-de-defensa.-total-provincia.-2022.csv": {
        "categoria": "reserva-natural-de-defensa",
    },
}

# Columnas "crudas" esperadas (con distintos posibles nombres)
NOMBRE_COLS = ["Nombre"]
MUNICIPIO_COLS = ["Municipio"]
TIPO_COLS = ["Tipo", "Tipo de reserva"]
SUPERFICIE_COLS = ["Superficie(Ha)", "Superficie"]


# --------------------------------------------------------------------
# 2. Helpers para normalizar columnas
# --------------------------------------------------------------------

def _find_first_existing_column(df: pd.DataFrame, candidates: List[str]) -> str:
    """
    Devuelve el primer nombre de columna que exista en el DataFrame
    de una lista de candidatos. Lanza ValueError si no encuentra ninguno.
    """
    # 1) búsqueda exacta
    for c in candidates:
        if c in df.columns:
            return c

    # 2) búsqueda insensible a mayúsculas/minúsculas (coincidencia exacta)
    cols_lower = {col.lower(): col for col in df.columns}
    for c in candidates:
        if c.lower() in cols_lower:
            return cols_lower[c.lower()]

    # 3) búsqueda por contener la palabra candidata en el nombre de columna
    def normalize(s: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", s.lower())

    normalized_cols = {normalize(col): col for col in df.columns}
    for c in candidates:
        nc = normalize(c)
        # buscar columnas que contengan nc o viceversa
        for ncol, orig in normalized_cols.items():
            if nc in ncol or ncol in nc:
                return orig

    # 4) intento de coincidencia por palabras clave simples (sin acentos)
    for c in candidates:
        key = re.sub(r"[^a-z0-9]+", "", c.lower())
        for col in df.columns:
            col_key = re.sub(r"[^a-z0-9]+", "", col.lower())
            if key in col_key or col_key in key:
                return col

    raise ValueError(f"Ninguna de las columnas candidatas existe: {candidates}")


def _sanitize_col_name(name: str) -> str:
    """Sanitize a column name to a safe identifier used in output CSV/JSON."""
    name = str(name).strip()
    # replace spaces and slashes with underscore, remove parentheses
    name = re.sub(r"[\s/\\]+", "_", name)
    name = re.sub(r"[()\[\],]", "", name)
    return name


def _normalizar_dataframe(df: pd.DataFrame, categoria: str, keep_extra_columns: bool = False) -> pd.DataFrame:
    """
    Toma un DataFrame crudo de un CSV y lo normaliza a columnas estándar:
    - nombre
    - municipio
    - tipo
    - superficie_ha
    - categoria
    """
    # Nombre (intento flexible)
    try:
        col_nombre = _find_first_existing_column(df, NOMBRE_COLS)
    except ValueError:
        # buscar columnas que contengan 'nombre' o 'reserva'
        candidates = [c for c in df.columns if re.search(r"nombre|reserva|name", c, flags=re.I)]
        if candidates:
            col_nombre = candidates[0]
        else:
            raise

    # Municipio / ubicación (más variantes: 'ubicación', 'localidad', 'partido')
    try:
        col_municipio = _find_first_existing_column(df, MUNICIPIO_COLS)
    except ValueError:
        candidates = [c for c in df.columns if re.search(r"ubic|localidad|partido|municipio|provincia|direccion", c, flags=re.I)]
        col_municipio = candidates[0] if candidates else None

    # Tipo
    try:
        col_tipo = _find_first_existing_column(df, TIPO_COLS)
    except ValueError:
        candidates = [c for c in df.columns if re.search(r"tipo|categoria|clase", c, flags=re.I)]
        col_tipo = candidates[0] if candidates else None

    # Superficie
    try:
        col_superficie = _find_first_existing_column(df, SUPERFICIE_COLS)
    except ValueError:
        candidates = [c for c in df.columns if re.search(r"superfic|ha|hect", c, flags=re.I)]
        col_superficie = candidates[0] if candidates else None

    # Construir el DataFrame normalizado con defensas si faltan columnas
    data = {}
    data["nombre"] = df[col_nombre].astype(str).str.strip() if col_nombre is not None else pd.Series([""] * len(df))
    data["municipio"] = df[col_municipio].astype(str).str.strip() if col_municipio is not None else pd.Series([""] * len(df))
    data["tipo"] = df[col_tipo].astype(str).str.strip() if col_tipo is not None else pd.Series([""] * len(df))
    data["superficie_ha"] = pd.to_numeric(df[col_superficie], errors="coerce") if col_superficie is not None else pd.Series([pd.NA] * len(df))

    df_norm = pd.DataFrame(data)

    # Agregamos la categoría desde el config
    df_norm["categoria"] = categoria

    # Limpiamos filas vacías (sin nombre o sin municipio y sin superficie)
    mask_not_empty = df_norm["nombre"].notna() & df_norm["nombre"].str.strip().ne("")
    df_norm = df_norm[mask_not_empty].copy()

    # Si solicitamos conservar columnas extra, las añadimos (con nombres sanitizados)
    if keep_extra_columns:
        mapped_cols = {col_nombre, col_municipio, col_tipo, col_superficie}
        for col in df.columns:
            if col in mapped_cols:
                continue
            sanitized = _sanitize_col_name(col)
            # evitar colisiones con columnas ya existentes
            base_name = sanitized
            suffix = 1
            while sanitized in df_norm.columns:
                sanitized = f"{base_name}_{suffix}"
                suffix += 1
            # Añadir la columna para las filas que quedaron en df_norm (alineando índices)
            try:
                df_norm[sanitized] = df.loc[df_norm.index, col].values
            except Exception:
                # fallback: asignar NaNs si algo raro ocurre
                df_norm[sanitized] = pd.NA

    # Opcional: quitar filas totalmente vacías en superficie (si querés)
    # df_norm = df_norm[df_norm["superficie_ha"].notna()]

    return df_norm



# --------------------------------------------------------------------
# 3. Función principal de carga y unificación
# --------------------------------------------------------------------

def cargar_reservas_desde_csv(base_path: str = ".") -> pd.DataFrame:
    """
    Carga todos los CSV definidos en CSV_CONFIG desde base_path,
    normaliza sus columnas y devuelve un único DataFrame unificado.

    base_path: carpeta donde están los CSV (por defecto, la carpeta actual).

    Devuelve:
        DataFrame con columnas:
        - nombre
        - municipio
        - tipo
        - superficie_ha
        - categoria
    """
    dataframes = []

    for filename, meta in CSV_CONFIG.items():
        ruta = os.path.join(base_path, filename)
        if not os.path.exists(ruta):
            print(f"[AVISO] No se encontró el archivo: {ruta}")
            continue

        print(f"[INFO] Cargando: {ruta}")
        df_raw = pd.read_csv(
            ruta,
            sep=";",
            encoding="latin1",
            engine="python",
        )

        df_norm = _normalizar_dataframe(df_raw, categoria=meta["categoria"], keep_extra_columns=True)
        dataframes.append(df_norm)

    # Buscar archivos Excel (.xlsx) en la carpeta y procesarlos también
    xlsx_paths = glob.glob(os.path.join(base_path, "*.xlsx"))
    for xpath in xlsx_paths:
        print(f"[INFO] Cargando XLSX: {xpath}")
        try:
            df_raw_xlsx = pd.read_excel(xpath, engine="openpyxl")
        except ValueError as e:
            # pandas raises ValueError if engine not found; give user guidance
            print(f"[ERROR] No se pudo leer {xpath}: {e}")
            print("Instalá 'openpyxl' en tu entorno: pip install openpyxl")
            continue
        except ImportError as e:
            print(f"[ERROR] Error al leer {xpath}: {e}")
            print("Instalá 'openpyxl' en tu entorno: pip install openpyxl")
            continue

        # Derivar una categoría a partir del nombre de archivo (si no se aporta)
        fname = os.path.splitext(os.path.basename(xpath))[0]
        categoria = _sanitize_col_name(fname).lower()

        df_norm_xlsx = _normalizar_dataframe(df_raw_xlsx, categoria=categoria, keep_extra_columns=True)
        dataframes.append(df_norm_xlsx)

    if not dataframes:
        raise RuntimeError("No se pudo cargar ningún CSV. Revisá las rutas.")

    df_all = pd.concat(dataframes, ignore_index=True)

    # Eliminamos duplicados básicos por nombre, municipio y categoría
    df_all = df_all.drop_duplicates(
        subset=["nombre", "municipio", "categoria"]
    ).reset_index(drop=True)

    return df_all


# --------------------------------------------------------------------
# 4. Export helpers (para n8n u otros)
# --------------------------------------------------------------------

def exportar_a_csv_y_json(df: pd.DataFrame,
                          csv_path: str = "reservas_unificadas.csv",
                          json_path: str = "reservas_unificadas.json") -> None:
    """
    Exporta el DataFrame unificado a CSV y JSON (lista de dicts),
    pensado para ser consumido desde n8n.

    En n8n:
      - Podés leer el CSV/JSON con un nodo 'Read Binary File' + 'Move Binary Data',
        o servirlo por HTTP desde algún servicio y usar 'HTTP Request' para traerlo.
    """
    print(f"[INFO] Guardando CSV en: {csv_path}")
    df.to_csv(csv_path, index=False, encoding="utf-8")

    print(f"[INFO] Guardando JSON en: {json_path}")
    records = df.to_dict(orient="records")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)


# --------------------------------------------------------------------
# 5. Punto de entrada (CLI)
# --------------------------------------------------------------------

if __name__ == "__main__":
    # Suponemos que los CSV están en la misma carpeta que este script.
    base_path = os.path.dirname(os.path.abspath(__file__))

    print("[INFO] Cargando y unificando reservas...")
    df_reservas = cargar_reservas_desde_csv(base_path=base_path)

    print("[INFO] Resumen:")
    print("  Total de filas:", len(df_reservas))
    print("  Categorías:")
    print(df_reservas["categoria"].value_counts())

    # Exportar archivos para n8n
    exportar_a_csv_y_json(
        df_reservas,
        csv_path=os.path.join(base_path, "reservas_unificadas.csv"),
        json_path=os.path.join(base_path, "reservas_unificadas.json"),
    )

    # Mostrar un pequeño sample
    print("\n[INFO] Ejemplo de filas:")
    print(df_reservas.head(10))
