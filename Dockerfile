# base docker image to build on
FROM python:3.13.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin

WORKDIR /code

ENV PATH="/code/.venv/bin:$PATH"


COPY pyproject.toml .python-version uv.lock ./
RUN uv sync --locked

COPY pipeline/pipeline.py .

ENTRYPOINT ["python", "pipeline.py"]





# RUN pip install pandas pyarrow

# COPY pipeline/pipeline.py .

# ENTRYPOINT ["uv", "run","python", "pipeline.py"]

## Use this for a more scalable system per GPT's recommendation, but it is more complex to set up and maintain. The above is a simpler version that should work for basic use cases.


# Use a slim Python base image
#FROM python:3.13.11-slim

# Install dependencies
#RUN pip install pandas pyarrow

# Set working directory inside the container
#WORKDIR /code

# Copy the entire pipeline folder into /code
#COPY pipeline/ ./pipeline/

# Set the entrypoint to run pipeline.py inside the pipeline folder
#ENTRYPOINT ["python", "pipeline/pipeline.py"]

