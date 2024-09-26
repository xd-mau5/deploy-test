# Usamos la imagen base de Python
FROM python:latest

# Instalamos cron y git
RUN apt-get update && apt-get install -y cron git

# Clonamos el repositorio en la carpeta /app
RUN git clone https://github.com/xd-mau5/deploy-test.git /app

# Establecemos el directorio de trabajo
WORKDIR /app

# Instalamos las dependencias de Python si tu proyecto tiene un requirements.txt
RUN pip install -r requirements.txt

# Copiamos el crontab file al contenedor
COPY crontab.txt /etc/cron.d/mycron

# Damos permisos al archivo de cron
RUN chmod 0644 /etc/cron.d/mycron

# Aplicamos el archivo de cron
RUN crontab /etc/cron.d/mycron

# Creamos el log file para cron
RUN touch /var/log/cron.log

# Iniciamos cron y el script
CMD cron && tail -f /var/log/cron.log
