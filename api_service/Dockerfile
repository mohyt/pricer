FROM python:3.9.13-slim
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
COPY pip_requirements /app/pip_requirements
RUN pip install --disable-pip-version-check --no-cache-dir --no-input --quiet --requirement /app/pip_requirements
COPY src /app
WORKDIR /app
EXPOSE 8088
CMD [ "/bin/bash", "-c", "gunicorn \"rest_server.main:rest_application(module_class_path='module.CostFetcherModule', service_name='api_service')\" --worker-class rest_server.uvicorn_worker.MiniDogUvicornWorker --bind 0.0.0.0:8088"]