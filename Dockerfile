# first stage
FROM python:3.9 AS builder
COPY requirements.txt .

# install dependencies to the local user directory (eg. /root/.local)
RUN pip install --user -r requirements.txt

# second unnamed stage
FROM python:3.9-slim

# set the working directory in the container
WORKDIR /code

# copy only the dependencies installation from the 1st stage image
COPY --from=builder /root/.local /root/.local
COPY ./src .

# update PATH environment variable
ENV PATH=/root/.local:$PATH
ENV PATH=/root/.local/bin:$PATH

CMD [ "python", "./server.py" ]
CMD [ "python", "./helium_main.py" ]


