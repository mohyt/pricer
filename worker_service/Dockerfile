FROM python:3.9.13-slim
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
COPY pip_requirements /app/pip_requirements
RUN pip install --disable-pip-version-check --no-cache-dir --no-input --quiet --requirement /app/pip_requirements
COPY src /app
WORKDIR /app
EXPOSE 8090
CMD [ "/bin/bash", "-c", "gunicorn \"rest_server.main:rest_application(module_class_path='module.WorkerModule', service_name='worker_service')\" --worker-class rest_server.uvicorn_worker.PricerUvicornWorker --bind 0.0.0.0:$PORT --timeout 120"]