import json
import os
from flask import Flask, request, jsonify, send_from_directory
from typing import Dict, Any, Optional
import unicodedata
import difflib

# ============================================================
# 1) Load the bot.json from your local folder
# ============================================================

BOT_FILE_PATH = "bot.txt"  # <-- change if your file is somewhere else

def load_bot_definition(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

bot_definition = load_bot_definition(BOT_FILE_PATH)
nodes_index = {node["id"]: node for node in bot_definition["nodes"]}
start_node_id = bot_definition.get("start_node_id", "inicio_menu")

# Cargar reservas unificadas para búsquedas rápidas
RESERVAS_PATH = os.path.join(os.path.dirname(__file__), "reservas_unificadas.json")
reservas_list = []
try:
    with open(RESERVAS_PATH, "r", encoding="utf-8") as rf:
        reservas_list = json.load(rf)
except Exception:
    reservas_list = []


def _normalize_name(s: Optional[str]) -> str:
    if not s:
        return ""
    s2 = str(s).strip().lower()
    s2 = unicodedata.normalize("NFKD", s2)
    s2 = "".join(ch for ch in s2 if not unicodedata.combining(ch))
    # remove punctuation except hyphen and underscore
    s2 = ''.join(ch if (ch.isalnum() or ch.isspace() or ch in "-_") else ' ' for ch in s2)
    s2 = ' '.join(s2.split())
    return s2




# ============================================================
# 2) Convert the bot node into the frontend format
# ============================================================

def node_to_payload(node: Dict[str, Any]) -> Dict[str, Any]:
    node_type = node.get("type", "response")
    options = node.get("options", [])

    formatted_options = [
        {"id": opt.get("id"), "label": opt.get("label", "Opción")}
        for opt in options
    ]

    return {
        "node_id": node.get("id"),
        "type": node_type,
        "message": node.get("message", ""),
        "options": formatted_options,
        "expects_input": (node_type == "input"),
        "is_end": (node_type == "end"),
    }


# ============================================================
# 3) Flow logic
# ============================================================

def resolve_next_node(current_node_id: str,
                      option_id: Optional[str],
                      user_input: Optional[str]) -> Dict[str, Any]:

    if current_node_id not in nodes_index:
        raise ValueError(f"Nodo '{current_node_id}' no encontrado")

    current_node = nodes_index[current_node_id]
    node_type = current_node.get("type")

    # MENU / RESPONSE → requires option_id
    if node_type in ("menu", "response"):
        if not option_id:
            raise ValueError("Falta 'option_id' para un nodo tipo menú/response")

        next_node_id = None
        for opt in current_node.get("options", []):
            if opt.get("id") == option_id:
                next_node_id = opt.get("next_node_id")
                break
        if not next_node_id:
            raise ValueError(f"Option '{option_id}' no existe en el nodo '{current_node_id}'")

        if next_node_id not in nodes_index:
            raise ValueError(f"El next_node_id '{next_node_id}' no existe en la definición del bot.")

        return nodes_index[next_node_id]

    # INPUT → only expects text from the user
    elif node_type == "input":
        next_node_id = current_node.get("next_node_id")
        if not next_node_id:
            raise ValueError(f"Nodo input '{current_node_id}' no tiene next_node_id")

        if next_node_id not in nodes_index:
            raise ValueError(f"El next_node_id '{next_node_id}' no existe en la definición del bot.")

        return nodes_index[next_node_id]

    # END → return itself
    elif node_type == "end":
        return current_node

    # Fallback
    else:
        next_node_id = current_node.get("next_node_id")
        return nodes_index.get(next_node_id, current_node)


# ============================================================
# 4) Flask API
# ============================================================

app = Flask(__name__)

# Try to enable CORS if available (optional dependency)
try:
    from flask_cors import CORS
    CORS(app)
except Exception:
    # flask-cors not installed; it's optional for local dev when serving frontend separately
    pass


@app.route("/api/start", methods=["POST"])
def api_start():
    """Return the starting node."""
    node = nodes_index.get(start_node_id)
    if node is None:
        return jsonify({"error": f"Nodo inicial '{start_node_id}' no encontrado."}), 500
    return jsonify(node_to_payload(node))


@app.route("/api/bot", methods=["GET"])
def api_bot():
    """Devuelve la definición completa del bot en formato JSON."""
    try:
        return jsonify(bot_definition)
    except Exception as e:
        return jsonify({"error": f"No se pudo devolver el JSON del bot: {e}"}), 500


@app.route("/api/reservas", methods=["GET"])
def api_reservas():
    """Buscar reservas por nombre. Query param: ?q=texto

    Devuelve una lista (máx 5) de coincidencias ordenadas por relevancia.
    """
    q = request.args.get("q", "") or ""
    q = q.strip()
    if not q:
        return jsonify([])

    nq = _normalize_name(q)
    # 1) coincidencias exactas normalizadas
    exact = [r for r in reservas_list if _normalize_name(r.get("nombre")) == nq]
    if exact:
        return jsonify(exact[:5])

    # 2) substring (contains) en nombre normalizado
    contains = [r for r in reservas_list if nq in _normalize_name(r.get("nombre"))]
    if contains:
        return jsonify(contains[:5])

    # 3) fuzzy match en nombres normalizados
    candidates = [_normalize_name(r.get("nombre")) for r in reservas_list]
    matches = difflib.get_close_matches(nq, candidates, n=5, cutoff=0.6)
    results = []
    for m in matches:
        for r in reservas_list:
            if _normalize_name(r.get("nombre")) == m:
                results.append(r)
                break

    return jsonify(results[:5])


@app.route("/api/step", methods=["POST"])
def api_step():
    """Advance the dialogue according to user choice/input."""
    data = request.get_json(force=True) or {}

    current_node_id = data.get("node_id")
    option_id = data.get("option_id")
    user_input = data.get("user_input")

    if not current_node_id:
        return jsonify({"error": "Missing 'node_id'"}), 400

    try:
        next_node = resolve_next_node(current_node_id, option_id, user_input)
        return jsonify(node_to_payload(next_node))
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": f"Error: {e}"}), 500


# ============================================================
# 5) Run the server locally
# ============================================================

if __name__ == "__main__":
    print("EcoGuía backend running on http://localhost:8000")
    # 추가: servir frontend estático y favicon para desarrollo local
    @app.route("/")
    def index():
        return send_from_directory(os.path.dirname(__file__), "eco_guia_frontend.html")

    @app.route("/favicon.ico")
    def favicon():
        # devolver 204 para evitar 404s en logs si no hay favicon
        return ("", 204)

    @app.route("/<path:filename>")
    def static_files(filename):
        return send_from_directory(os.path.dirname(__file__), filename)

    app.run(host="0.0.0.0", port=8000, debug=True)
