FROM python:3.11

WORKDIR /app

COPY ./code/requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt

COPY ./code/ /app/

COPY --from=solution_config ./analysis_modules /app/analysis_modules
COPY --from=solution_config ./module_config /app/module_config

CMD ["python","main.py"]
