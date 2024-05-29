FROM public.ecr.aws/lambda/python:3.11

# copy the requirements
COPY requirements.txt .

# install the dependencies
RUN pip install -r requirements.txt

# copy the function code
COPY scraper_lambda_fuction.py .

# set the CMD to the handler
CMD [ 'scraper_lambda_fuction.lambda_handler' ]