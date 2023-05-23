
import os
import requests
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime,timedelta
from flask import Flask, jsonify, request
from matplotlib.ticker import FuncFormatter
from sqlalchemy import create_engine, text, MetaData, Table

app = Flask(__name__)

engine = create_engine('postgresql://postgres:ZKqBxNKa5wCs3Qc6ckmw@containers-us-west-156.railway.app:7633/railway')
tabla = 'red_electrica'

@app.route('/')
def index():
    return jsonify("<h1>Conexión disponible</h1>")

@app.route('/get_demand')
def db():
    #PETICION API REE

    dataframes = []
    group_days = 25

    current_date = datetime.fromisoformat(request.args['fecha_inicio'])
    datetime_fin = datetime.fromisoformat(request.args['fecha_fin'])

    while current_date <= datetime_fin:
        fecha_inicio_actual = current_date
        fecha_fin_actual = current_date + timedelta(days=group_days)

        if fecha_fin_actual > datetime_fin:
            fecha_fin_actual = datetime_fin

        url = f"https://apidatos.ree.es/en/datos/demanda/evolucion?start_date={fecha_inicio_actual.strftime('%Y-%m-%dT%H:%M')}&end_date={fecha_fin_actual.strftime('%Y-%m-%dT%H:%M')}&time_trunc=hour&geo_trunc=electric_system&geo_limit=peninsular&geo_ids=8741"

        response = requests.get(url)
        data = response.json()

        if int(str(response).split("[")[1].split("]")[0]) == 200:
            raw = data["included"][0]["attributes"]["values"]

            value_list = [round(float(r["value"]), 3) for r in raw]
            date_list = [datetime.fromisoformat(r["datetime"].split(".")[0]) for r in raw]
            year_list = [int(r["datetime"].split("-")[0]) for r in raw]
            month_list = [int(r["datetime"].split("-")[1]) for r in raw]
            day_list = [int(r["datetime"].split("-")[2].split("T")[0]) for r in raw]
            hour_list = [int(r["datetime"].split("T")[1].split(".")[0].split(":")[0]) for r in raw]

            dicc_electric_demand = {
                "value": value_list,
                "date": date_list,
                "year": year_list,
                "month": month_list,
                "day": day_list,
                "hour": hour_list
            }
            df_electric_demand = pd.DataFrame(dicc_electric_demand)
            dataframes.append(df_electric_demand)

        else:
            print(response)
            print(data)
            break

        current_date += timedelta(days=group_days)
        current_date += timedelta(days=1)  # Agregar 1 día para evitar solapamiento

    df = pd.concat(dataframes, ignore_index=True)

    time = request.args['time']
    plot_type = request.args['plot_type']

    plt.figure(figsize=(6.5, 4.5))

    if time == "year":
        title = "Años"
    elif time == "month":
        title = "Meses del año"
    elif time == "day":
        title = "Días del mes"
    elif time == "hour":
        title = "Horas del día"

    if plot_type == "barplot":
        sns.barplot(data=df, x=time, y="value", errorbar=None, color= "blue")
    elif plot_type == "lineplot":
        sns.lineplot(data=df, x=time, y="value", errorbar=None, marker="o", color= "blue")

    # Función personalizada para formatear los valores del eje y
    def format_y_axis(value, _):
        if value >= 1000:
            value = f"{value/1000:.0f}K"
        return value

    # Ajustar el formato de los valores del eje y
    formatter = FuncFormatter(format_y_axis)
    plt.gca().yaxis.set_major_formatter(formatter)

    plt.xlabel(f"{title}")
    plt.xticks(fontsize=9)
    plt.ylabel("Consumo (MW)")
    plt.yticks(fontsize=9)
    plt.title(f"Demanda eléctrica peninsular por {title}")
    plt.show()
     
    # Insertar base de datos PostgreSQL
    df_existente = pd.read_sql_table(tabla, con=engine)
    df_combinado = pd.concat([df_existente, df], ignore_index=True)
    df_sin_duplicados = df_combinado.drop_duplicates()
    df_sin_duplicados.to_sql(tabla, con=engine, if_exists='append', index=False)

    return f'Registros insertados'

@app.route('/get_db_data')
def get_db_data():
    
    df = pd.DataFrame(pd.read_sql_query(text("""SELECT value, date FROM red_electrica"""), con=engine.connect()))

    # Generar la tabla HTML con estilos
    html_table = '<table style="border-collapse: collapse; width: 100%;">'
    html_table += '<tr style="background-color: lightgray; text-align: center;">'
    for column in df.columns:
        html_table += f'<th style="border: 1px solid black; padding: 8px;">{column}</th>'
    html_table += '</tr>'
    for _, row in df.iterrows():
        html_table += '<tr>'
        for value in row:
            html_table += f'<td style="border: 1px solid black; padding: 8px; text-align: center;">{value}</td>'
        html_table += '</tr>'
    html_table += '</table>'

    # Devolver la respuesta HTML
    return html_table

@app.route('/wipe_data')
def borrar_tabla():
    secret = request.args.get('secret')

    if secret == 'borrar':

        # Ejecutar la consulta para borrar el contenido de la tabla
        with engine.connect() as connection:
            connection.execute(text(f"DELETE FROM {tabla}"))

        # Leer los registros de la tabla después de borrar
        with engine.connect() as connection:
            registros = pd.read_sql_query(f"SELECT COUNT(*) FROM {tabla}", con=connection)

        return f"<h1>El contenido de la tabla '{tabla}' ha sido borrado.</h1> Registros restantes: {registros}"
    else:
        return "<h1>Palabra incorrecta. No se ha borrado nada.</h1>"
 
if __name__ == '__main__':
    app.run(debug=True, port=os.getenv("PORT", default=5000))

