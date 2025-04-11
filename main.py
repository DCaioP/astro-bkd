from flask import Flask, request, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# Configurações do banco de dados
DB_HOST = '127.0.0.1'
DB_NAME = 'astro'
DB_USER = 'caiop'
DB_PASS = 'asdf'
DB_PORT = 5432


def get_db_connection():
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        port=DB_PORT
    )
    return conn


@app.route('/data', methods=['GET'])
def get_data():
    # Obter parâmetros de filtro da query string
    month = request.args.get('month')
    machine = request.args.get('machine')
    shift = request.args.get('shift')

    query_filters = []
    query_params = []

    if month:
        query_filters.append("EXTRACT(MONTH FROM \"Data de Conclusão\") = %s")
        query_params.append(month)
    if machine:
        query_filters.append("\"Nome do Equipamento\" = %s")
        query_params.append(machine)
    if shift:
        query_filters.append("\"Turno\" = %s")
        query_params.append(shift)

    where_clause = ''
    if query_filters:
        where_clause = 'WHERE ' + ' AND '.join(query_filters)

    query = f"""
        SELECT 
            "Data de Conclusão" AS data_conclusao,
            "Hora Ouro" AS ho_diaria_realizada,
            "hora ouro acumulada" AS ho_acumulada,
            "Hora Ouro Meta" AS ho_meta_diaria,
            "hora ouro meta acumulada" AS ho_meta_acumulada
        FROM astro.public.aggregate_data
        {where_clause}
        ORDER BY "Data de Conclusão" ASC
    """

    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(query, query_params)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        # Converter datas para strings no formato ISO
        for row in rows:
            row['data_conclusao'] = row['data_conclusao'].isoformat()

        return jsonify(rows)
    except Exception as e:
        print(f"Erro ao buscar dados: {e}")
        return jsonify({'error': 'Erro ao buscar dados'}), 500


if __name__ == '__main__':
    app.run(debug=True, port=5000)
