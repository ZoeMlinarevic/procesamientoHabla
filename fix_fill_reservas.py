import json
from pathlib import Path
import pandas as pd
import unicodedata
import re
import difflib

BASE = Path(__file__).parent
JPATH = BASE / "reservas_unificadas.json"
CPATH = BASE / "reservas_unificadas.csv"

# Leer JSON que puede contener NaN literales (no válido JSON).
text = JPATH.read_text(encoding="utf-8")
text = text.replace("NaN", "null")
records = json.loads(text)


# Normalización de nombres para matching leniente
def normalize_name(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = s.strip()
    if s.lower() in ("nan", "none", "null", ""):
        return ""
    # NFKD y eliminar acentos
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    # reemplazar paréntesis por espacio, eliminar puntuación básica
    s = re.sub(r"[\(\)\[\]{}<>\"]", " ", s)
    s = re.sub(r"[^\w\s\-]", " ", s)
    # colapsar espacios y minúsculas
    s = re.sub(r"\s+", " ", s)
    return s.lower().strip()


# Mapeo de nombre -> campos a rellenar
mapping = {
    "Puerto de Mar del Plata": {
        "Horarios_de_visita": "Todos los días 10:00–18:00",
        "Costo_de_ingreso": "Gratuito",
        "Fuente": "Ministerio de Ambiente PBA",
    },
    "Laguna de los Padres": {
        "Horarios_de_visita": "Todos los días 08:00–20:00",
        "Costo_de_ingreso": "Gratuito",
        "Fuente": "MGP / La Capital MdP",
    },
    "Bahía de  Samborombón (Sitio RAMSAR)": {
        "Horarios_de_visita": "No abierta al público (área natural amplia sin horario)",
        "Costo_de_ingreso": "No corresponde",
        "Fuente": "OPDS – Listado oficial",
    },
    "Rincón de Ajó": {
        "Horarios_de_visita": "No abierta al público (reserva estricta sin visitas)",
        "Costo_de_ingreso": "No corresponde",
        "Fuente": "Min. Ambiente PBA",
    },
    "Laguna Chasicó": {
        "Horarios_de_visita": "No abierta al público (acceso libre para pesca deportiva)",
        "Costo_de_ingreso": "Gratuito (con licencia de pesca)",
        "Fuente": "OPDS – Listado oficial",
    },
    "Otamendi": {
        "Horarios_de_visita": "Todos los días 09:00–18:00 (ingreso diurno)",
        "Costo_de_ingreso": "Gratuito",
        "Fuente": "La Nación",
    },
    "Parque nacional Campos del Tuyú": {
        "Horarios_de_visita": "No abierta al público aún",
        "Costo_de_ingreso": "No corresponde",
        "Fuente": "Argentina.gob.ar",
    },
    "Costera Bahia Blanca": {
        "Horarios_de_visita": "Sin horarios fijos (visitas guiadas ocasionales)",
        "Costo_de_ingreso": "Gratuito",
        "Fuente": "Aves Argentinas (consulta turismo)",
    },
    "Paraná Guazú": {
        "Horarios_de_visita": "No abierta al público (islas del delta sin acceso turístico)",
        "Costo_de_ingreso": "No corresponde",
        "Fuente": "OPDS – Listado oficial",
    },
    "Guardia del Juncal": {
        "Horarios_de_visita": "No abierta al público (reserva educativa cerrada)",
        "Costo_de_ingreso": "No corresponde",
        "Fuente": "–",
    },
    "Pehuencó- Monte Hermoso": {
        "Horarios_de_visita": "Visitas solo con guía especializada, según mareas (coordinar con Museo en Monte Hermoso)",
        "Costo_de_ingreso": "ARS 4.000 adultos / ARS 3.000 menores (visita guiada)",
        "Fuente": "Municipio de Monte Hermoso; Infocielo",
    },
    "Ingeniero Maschwitz": {
        "Horarios_de_visita": "Miércoles a domingos 09:00–17:00 (cerrado lluvia)",
        "Costo_de_ingreso": "Gratuito",
        "Fuente": "Reserva Maschwitz (Escobar)",
    },
    "Restinga del Faro": {
        "Horarios_de_visita": "Sin horarios (acceso libre a la costa, sin servicios)",
        "Costo_de_ingreso": "Gratuito",
        "Fuente": "–",
    },
    "Costanera Sur (Mar del Plata)": {
        "Horarios_de_visita": "Sin horarios (área pública, acceso libre)",
        "Costo_de_ingreso": "Gratuito",
        "Fuente": "–",
    },
    "Laguna de San Vicente": {
        "Horarios_de_visita": "Sin datos (posiblemente abierta días festivos locales)",
        "Costo_de_ingreso": "Gratuito",
        "Fuente": "–",
    },
    "Sierra del Tigre": {
        "Horarios_de_visita": "Invierno 09:30–17:30; Verano 09:00–19:00 (cerrado lluvias)",
        "Costo_de_ingreso": "ARS 5.000 por persona (menores 10 años gratis)",
        "Fuente": "Reserva Sierra del Tigre",
    },
    "Dr. Carlos Spegazzini": {
        "Horarios_de_visita": "No abierta al público (área de investigación)",
        "Costo_de_ingreso": "No corresponde",
        "Fuente": "–",
    },
    "Selva marginal de Hudson": {
        "Horarios_de_visita": "No abierta al público (solo investigación/educación)",
        "Costo_de_ingreso": "No corresponde",
        "Fuente": "–",
    },
    "Bahias Blanca Verde y Falsa": {
        "Horarios_de_visita": "Sin horarios fijos (acceso con autorización especial o excursión)",
        "Costo_de_ingreso": "Gratuito",
        "Fuente": "Min. Ambiente PBA",
    },
    "Rio Luján": {
        "Horarios_de_visita": "No abierta al público (zona protegida, sin uso recreativo)",
        "Costo_de_ingreso": "No corresponde",
        "Fuente": "–",
    },
    "Arroyo  Los Gauchos": {
        "Horarios_de_visita": "No abierta al público",
        "Costo_de_ingreso": "No corresponde",
        "Fuente": "–",
    },
    "Laguna Salada Grande": {
        "Horarios_de_visita": "Sábados, domingos y feriados 10:00–17:00",
        "Costo_de_ingreso": "Gratuito (permiso de pesca requerido)",
        "Fuente": "Min. Ambiente PBA",
    },
    "Isla Martín García": {
        "Horarios_de_visita": "Servicio de lancha los martes, jueves, sábados, domingos y feriados (sale 08:30 h de Tigre, regresa ~20:00 h)",
        "Costo_de_ingreso": "Gratuito (traslado en lancha pago)",
        "Fuente": "Min. Ambiente PBA",
    },
    "Mar Chiquita": {
        "Horarios_de_visita": "Todos los días 09:00–17:00",
        "Costo_de_ingreso": "Gratuito",
        "Fuente": "Min. Ambiente PBA",
    },
    "Arroyo Zabala": {
        "Horarios_de_visita": "No abierta al público",
        "Costo_de_ingreso": "No corresponde",
        "Fuente": "–",
    },
    "Bahia San Blas": {
        "Horarios_de_visita": "Sin horarios (acceso libre, actividad de pesca deportiva)",
        "Costo_de_ingreso": "Gratuito",
        "Fuente": "–",
    },
    "Isla de Puan": {
        "Horarios_de_visita": "Visitas mediante lanchas turísticas (según programación local)",
        "Costo_de_ingreso": "Gratuito (traslado en lancha pago)",
        "Fuente": "Turismo Puan (redes sociales locales)",
    },
    "Isla Botija": {
        "Horarios_de_visita": "No abierta al público",
        "Costo_de_ingreso": "No corresponde",
        "Fuente": "–",
    },
    "Islote de la Gaviota Cangrejera": {
        "Horarios_de_visita": "No abierta al público (isla protegida, acceso prohibido)",
        "Costo_de_ingreso": "No corresponde",
        "Fuente": "Min. Ambiente PBA",
    },
    "Punta Lara": {
        "Horarios_de_visita": "Visitas solo con guía (turno previo por email; sin ingreso libre)",
        "Costo_de_ingreso": "Gratuito",
        "Fuente": "Min. Ambiente PBA; La Otra Cara",
    },
    "Laguna de Rocha": {
        "Horarios_de_visita": "No abierta al público (acceso solo con permiso especial)",
        "Costo_de_ingreso": "No corresponde",
        "Fuente": "Ley Prov. 14.488 (creación reserva)",
    },
    "Isla Laguna Alsina": {
        "Horarios_de_visita": "No abierta al público",
        "Costo_de_ingreso": "No corresponde",
        "Fuente": "–",
    },
    "Delta en formación": {
        "Horarios_de_visita": "No abierta al público",
        "Costo_de_ingreso": "No corresponde",
        "Fuente": "–",
    },
    "Bahia de Samborombón (núcleo)": {
        "Horarios_de_visita": "No abierta al público",
        "Costo_de_ingreso": "No corresponde",
        "Fuente": "OPDS – Sitio RAMSAR",
    },
    "Parque ecológico Islas 1ª Sección (Delta)": {
        "Horarios_de_visita": "No abierta al público (uso científico/educativo)",
        "Costo_de_ingreso": "No corresponde",
        "Fuente": "–",
    },
    "Pereyra Iraola": {
        "Horarios_de_visita": "Abierto lun a dom 09:00–20:00",
        "Costo_de_ingreso": "Gratuito",
        "Fuente": "Meteored",
    },
    "Parque Costero del Sur": {
        "Horarios_de_visita": "Sin horarios fijos (acceso libre a senderos; visitas guiadas disponibles)",
        "Costo_de_ingreso": "Gratuito",
        "Fuente": "UNESCO – RB Parque Costero Sur",
    },
    "Parque Atlántico Mar Chiquito": {
        "Horarios_de_visita": "Todos los días 09:00–17:00 (Centro de visitantes)",
        "Costo_de_ingreso": "Gratuito",
        "Fuente": "Min. Ambiente PBA",
    },
    "Delta del Paraná": {
        "Horarios_de_visita": "Sin horarios (región del delta habitada; excursiones privadas)",
        "Costo_de_ingreso": "Gratuito",
        "Fuente": "UNESCO – RB Delta Paraná (cit. Instagram)",
    },
    "Barranca Norte": {
        "Horarios_de_visita": "No abierta al público (reserva privada sin visitas)",
        "Costo_de_ingreso": "No corresponde",
        "Fuente": "Min. Ambiente PBA",
    },
    "El Morejón": {
        "Horarios_de_visita": "No abierta al público",
        "Costo_de_ingreso": "No corresponde",
        "Fuente": "–",
    },
    "Paitití": {
        "Horarios_de_visita": "No abierta al público",
        "Costo_de_ingreso": "No corresponde",
        "Fuente": "–",
    },
    "El Destino": {
        "Horarios_de_visita": "Mar–Nov: todos los días 08:00–18:00 (miércoles cerrado); Dic–Feb: todos los días 08:00–20:00",
        "Costo_de_ingreso": "ARS 6.000 entrada por persona; camping ARS 12.000; actividades extra desde ARS 6.000",
        "Fuente": "Infocielo; Sitio oficial",
    },
    "Nahuel Rucá": {
        "Horarios_de_visita": "No abierta al público (campo privado)",
        "Costo_de_ingreso": "No corresponde",
        "Fuente": "–",
    },
    "Arroyo Durazno": {
        "Horarios_de_visita": "No abierta al público",
        "Costo_de_ingreso": "No corresponde",
        "Fuente": "–",
    },
    "La Amanda": {
        "Horarios_de_visita": "No abierta al público",
        "Costo_de_ingreso": "No corresponde",
        "Fuente": "–",
    },
    "Punta Indio (Estancia)": {
        "Horarios_de_visita": "No abierta al público",
        "Costo_de_ingreso": "No corresponde",
        "Fuente": "–",
    },
    "Las Piedras": {
        "Horarios_de_visita": "No abierta al público",
        "Costo_de_ingreso": "No corresponde",
        "Fuente": "–",
    },
    "Santuario del Cauquén Colorado": {
        "Horarios_de_visita": "No abierta al público (área protegida de uso científico)",
        "Costo_de_ingreso": "No corresponde",
        "Fuente": "–",
    },
    "Che Roga": {
        "Horarios_de_visita": "No abierta al público",
        "Costo_de_ingreso": "No corresponde",
        "Fuente": "–",
    },
    "Sendero Pampa": {
        "Horarios_de_visita": "Visitas educativas programadas (consultar universidad)",
        "Costo_de_ingreso": "Gratuito (actividades educativas)",
        "Fuente": "UNICEN",
    },
    "Sierras Grandes": {
        "Horarios_de_visita": "No abierta al público (acceso solo con autorización)",
        "Costo_de_ingreso": "No corresponde",
        "Fuente": "Min. Ambiente PBA (reg. OPDS)",
    },
    "Delta Terra": {
        "Horarios_de_visita": "Sábados, domingos y feriados 10:00–17:30 (abre fines de semana)",
        "Costo_de_ingreso": "Gratuito (traslado en lancha con costo)",
        "Fuente": "Fundación Azara",
    },
    "Achalay": {
        "Horarios_de_visita": "No abierta al público",
        "Costo_de_ingreso": "No corresponde",
        "Fuente": "–",
    },
    "Laguna Epecuén": {
        "Horarios_de_visita": "Abierto todos los días (acceso libre, ruinas al aire libre)",
        "Costo_de_ingreso": "Gratuito",
        "Fuente": "Turismo PBA (ref. Magdalena, sim.)",
    },
    "La Saladita": {
        "Horarios_de_visita": "Sin datos (proyecto de reserva en humedal, sin acceso formal)",
        "Costo_de_ingreso": "–",
        "Fuente": "–",
    },
    "Parque del Este": {
        "Horarios_de_visita": "Abierto todos los días (espacio público)",
        "Costo_de_ingreso": "Gratuito",
        "Fuente": "Turismo PBA (contexto Baradero)",
    },
    "Punta Rasa": {
        "Horarios_de_visita": "Acceso libre todos los días (24h, salvo restricciones Prefectura)",
        "Costo_de_ingreso": "Gratuito",
        "Fuente": "El Día (contexto Punta Rasa)",
    },
    "Parque Ecológico Municipal (La Plata)": {
        "Horarios_de_visita": "Lunes a domingo 09:00–19:00",
        "Costo_de_ingreso": "Gratuito",
        "Fuente": "MLP / Facebook",
    },
    "Santa Catalina": {
        "Horarios_de_visita": "No abierta al público (predio de reserva educativa)",
        "Costo_de_ingreso": "No corresponde",
        "Fuente": "–",
    },
    "Quinta de Cigordia": {
        "Horarios_de_visita": "Todos los días 08:00–19:00",
        "Costo_de_ingreso": "Gratuito",
        "Fuente": "BuenosAires123",
    },
    "Del Pilar": {
        "Horarios_de_visita": "Todos los días 09:00–18:00",
        "Costo_de_ingreso": "Gratuito",
        "Fuente": "Reserva Pilar (Facebook)",
    },
    "Bajo de Bordenave": {
        "Horarios_de_visita": "No abierta al público",
        "Costo_de_ingreso": "No corresponde",
        "Fuente": "–",
    },
    "Selva Marginal Quilmeña": {
        "Horarios_de_visita": "No abierta libremente (recorridas guiadas esporádicas)",
        "Costo_de_ingreso": "Gratuito",
        "Fuente": "–",
    },
    "Ramallo": {
        "Horarios_de_visita": "Sin datos (espacio natural sin infraestructura)",
        "Costo_de_ingreso": "Gratuito",
        "Fuente": "–",
    },
    "Ribera Norte": {
        "Horarios_de_visita": "Lunes a viernes 09:00–17:00; sábados, domingos 09:00–18:00",
        "Costo_de_ingreso": "Gratuito",
        "Fuente": "Mun. San Isidro",
    },
    "Barranca de la Quinta Los Ombues": {
        "Horarios_de_visita": "Martes a domingos 13:00–19:00 (horario de museo histórico)",
        "Costo_de_ingreso": "Gratuito",
        "Fuente": "Mun. San Isidro (Facebook)",
    },
    "Barranca de la Quinta Pueyrredon": {
        "Horarios_de_visita": "Martes a domingos 10:00–18:00 (horario de parque/museo)",
        "Costo_de_ingreso": "Gratuito",
        "Fuente": "San Isidro Cultura (sitio oficial, horario museos)",
    },
    "Rafael de Aguiar": {
        "Horarios_de_visita": "Abierto todo el año (acceso público sin restricción horaria)",
        "Costo_de_ingreso": "Gratuito",
        "Fuente": "Clarín (puesta en valor recreativa)",
    },
    "Vuelta de Obligado": {
        "Horarios_de_visita": "Todos los días 09:00–18:00 (aprox., horario diurno)",
        "Costo_de_ingreso": "Gratuito",
        "Fuente": "Turismo PBA (mención circuito)",
    },
    # Añadir más si necesitás...
}


# Preparar índices normalizados para matching rápido
norm_to_key = {}
for k in mapping:
    nk = normalize_name(k)
    if nk:
        norm_to_key[nk] = k

updated = 0
not_found = []
fuzzy_matches = []
for rec in records:
    name = rec.get("nombre")
    if not isinstance(name, str):
        not_found.append(name)
        continue
    # primer intento: exacto directo
    if name in mapping:
        vals = mapping[name]
        for kk, vv in vals.items():
            rec[kk] = vv
        updated += 1
        continue

    # segundo intento: normalized exact
    nname = normalize_name(name)
    if nname and nname in norm_to_key:
        mk = norm_to_key[nname]
        for kk, vv in mapping[mk].items():
            rec[kk] = vv
        updated += 1
        continue

    # tercer intento: fuzzy match against normalized keys
    if nname:
        candidates = list(norm_to_key.keys())
        close = difflib.get_close_matches(nname, candidates, n=1, cutoff=0.75)
        if close:
            matched_norm = close[0]
            mk = norm_to_key[matched_norm]
            for kk, vv in mapping[mk].items():
                rec[kk] = vv
            updated += 1
            fuzzy_matches.append((name, mk))
            continue

    not_found.append(name)

print(f"Updated {updated} records. ")
if fuzzy_matches:
    print(f"Fuzzy matches applied (sample 10): {fuzzy_matches[:10]}")
if not_found:
    print(f"Not matched samples (up to 20): {not_found[:20]}")

# Guardar JSON y CSV actualizados
JPATH.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
# También reescribimos CSV con pandas para consistencia
pd.DataFrame(records).to_csv(CPATH, index=False, encoding="utf-8")
print("Wrote updated JSON and CSV.")
