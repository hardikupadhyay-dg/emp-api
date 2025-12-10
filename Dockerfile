FROM public.ecr.aws/lambda/python:3.11

# Copy function code
COPY lambda_function.py ${LAMBDA_TASK_ROOT}

# Command can be overridden by AWS Lambda runtime environment
CMD ["lambda_function.lambda_handler"]
