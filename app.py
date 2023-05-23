
import os
import io
import base64
import requests
import pandas as pd
from PIL import Image
import plotly.io as pio
import plotly.express as px
import matplotlib.pyplot as plt
from datetime import datetime,timedelta
from sqlalchemy import create_engine, text
from flask import Flask, request, send_file, render_template

app = Flask(__name__)

engine = create_engine('postgresql://postgres:ZKqBxNKa5wCs3Qc6ckmw@containers-us-west-156.railway.app:7633/railway')
tabla = 'red_electrica'

@app.route('/')
def index():
    return render_template('index.html')

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

    # Insertar en base de datos PostgreSQL
    df_existente = pd.read_sql_table(tabla, con=engine)
    df_combinado = pd.concat([df_existente, df], ignore_index=True)
    df_sin_duplicados = df_combinado.drop_duplicates()
    df_sin_duplicados.to_sql(tabla, con=engine, if_exists='append', index=False)

    time = request.args['time']
    plot_type = request.args['plot_type']

    plt.figure(figsize=(6.5, 4.5))

    buffer = io.BytesIO()

    if time == "year":
        title = "Años"
    elif time == "month":
        title = "Meses del año"
    elif time == "day":
        title = "Días del mes"
    elif time == "hour":
        title = "Horas del día"

    if plot_type == "barplot":
        fig = px.bar(df, x=time, y="value")
        fig.update_layout(
            title=f"Demanda eléctrica peninsular por {title}",
            xaxis_title=title,
            title_font=dict(color='white', size=8),
            yaxis_title="Consumo (MW)",
            font=dict(size=9),
            plot_bgcolor='rgba(0, 0, 0, 0)',
            paper_bgcolor='rgba(0, 0, 0, 0)',
            xaxis=dict(title_font=dict(size=6), tickfont=dict(size=4), dtick=1),
            yaxis=dict(title_font=dict(size=6), tickfont=dict(size=4), tickformat=".2s"),
            margin=dict(t=20),
            title_x=0.75,  # Alinear el título a la derecha
            title_y=0.95)  # Alinear el título hacia la parte superior del gráfico)
            # Configurar opciones de estilo para las etiquetas y títulos de los ejes
              # Ajustar el margen superior (valor negativo para reducir la distancia)
        fig.update_xaxes(title_font=dict(color='white'), tickfont=dict(color='white'))
        fig.update_yaxes(title_font=dict(color='white'), tickfont=dict(color='white'))
        pio.write_image(fig, buffer, format='png', scale=3, width=400, height=250)
       
    elif plot_type == "lineplot":
        df_sum = df.groupby(time)["value"].sum().reset_index()
        fig = px.line(df_sum, x=time, y="value", markers=True)
        fig.update_traces(line=dict(width=2)) 
        fig.update_layout(
            title=f"Demanda eléctrica peninsular por {title}",
            xaxis_title=title,
            title_font=dict(color='white', size=8),
            yaxis_title="Consumo (MW)",
            font=dict(size=9),
            plot_bgcolor='rgba(0, 0, 0, 0)',
            paper_bgcolor='rgba(0, 0, 0, 0)',
            xaxis=dict(title_font=dict(size=6), tickfont=dict(size=4), dtick=1, showline=True, zeroline=True),
            yaxis=dict(title_font=dict(size=6), tickfont=dict(size=4), tickformat=".2s"),
            xaxis_showgrid=False,
            margin=dict(t=20),
            title_x=0.75,  # Alinear el título a la derecha
            title_y=0.95)  # Oculta las líneas verticales del grid        
        # Limitar el eje x
        x_min = df_sum[time].min()
        x_max = df_sum[time].max()
        # Configurar opciones de estilo para las etiquetas y títulos de los ejes
        fig.update_xaxes(title_font=dict(color='white'), tickfont=dict(color='white'), range=[x_min-0.2, x_max+0.5])
        fig.update_yaxes(title_font=dict(color='white'), tickfont=dict(color='white'))
    pio.write_image(fig, buffer, format='png', scale=3, width=400, height=250)
    
    buffer.seek(0)

    # Cargar la imagen original desde el búfer de Bytes
    imagen_original = Image.open(buffer)

    # Obtener los datos de la imagen original en formato PNG
    imagen_data = io.BytesIO()
    imagen_original.save(imagen_data, format='PNG')
    imagen_data.seek(0)

    # Codificar la imagen original en Base64
    imagen_codificada = base64.b64encode(imagen_data.getvalue()).decode("utf-8")

    return render_template('image.html', imagen_codificada=imagen_codificada)


@app.route('/get_db_data')
def get_db_data():
    df = pd.DataFrame(pd.read_sql_query(text("""SELECT value, date FROM red_electrica"""), con=engine.connect()))

    # Generar el código HTML de la tabla con aspecto de DataFrame de Pandas
    html_table = df.to_html(classes='pandas-table', index=False)

    # Renderizar el archivo HTML con los datos de la tabla
    return render_template('tabla.html', tabla=html_table)

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

