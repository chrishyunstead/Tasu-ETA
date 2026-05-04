FROM public.ecr.aws/lambda/python:3.11

RUN yum install -y libgomp && yum clean all

COPY requirements.txt .
RUN python -m pip install --upgrade pip \
    && pip install --no-cache-dir --only-binary=:all: -r requirements.txt

COPY . ${LAMBDA_TASK_ROOT}

CMD ["app.lambda_handler"]
