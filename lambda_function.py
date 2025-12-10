import json
import os
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

DDB = boto3.resource("dynamodb")
TABLE_NAME = os.getenv("EMP_TABLE_NAME", "Emp_Master")
TABLE = DDB.Table(TABLE_NAME)


def _build_response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps(body)
    }


def handle_post_employee(body):
    required_fields = ["Emp_Id", "First_Name", "Last_Name", "Date_Of_Joining"]
    missing = [f for f in required_fields if f not in body]
    if missing:
        return _build_response(
            400,
            {"error": f"Missing fields: {', '.join(missing)}"}
        )

    item = {
        "Emp_Id": str(body["Emp_Id"]),
        "First_Name": str(body["First_Name"]),
        "Last_Name": str(body["Last_Name"]),
        "Date_Of_Joining": str(body["Date_Of_Joining"]),
        "Created_At": datetime.utcnow().isoformat()
    }

    try:
        TABLE.put_item(Item=item)
    except ClientError as exc:
        print(f"DynamoDB put_item error: {exc}")
        return _build_response(500, {"error": "Failed to create employee"})

    print(f"Employee created: {item}")
    return _build_response(201, item)


def handle_get_employee(emp_id):
    try:
        resp = TABLE.get_item(Key={"Emp_Id": str(emp_id)})
    except ClientError as exc:
        print(f"DynamoDB get_item error: {exc}")
        return _build_response(500, {"error": "Failed to fetch employee"})

    item = resp.get("Item")
    if not item:
        return _build_response(404, {"error": "Employee not found"})

    print(f"Employee fetched: {item}")
    return _build_response(200, item)


def lambda_handler(event, context):
    """
    Handle both local testing and API Gateway proxy events.
    """
    # Local test path
    if "httpMethod" not in event and "requestContext" not in event:
        action = event.get("action")
        if action == "create":
            return handle_post_employee(event.get("body", {}))
        if action == "get":
            return handle_get_employee(event.get("emp_id"))
        return _build_response(400, {"error": "Unknown local action"})

    # API Gateway HTTP proxy style
    http_method = event.get("httpMethod") or event.get("requestContext", {}).get("http", {}).get("method")
    raw_path = event.get("path") or event.get("rawPath", "")

    if raw_path.endswith("/employee"):
        if http_method == "POST":
            body = event.get("body") or "{}"
            if event.get("isBase64Encoded"):
                body = base64.b64decode(body).decode("utf-8")
            try:
                payload = json.loads(body)
            except json.JSONDecodeError:
                return _build_response(400, {"error": "Invalid JSON payload"})

            return handle_post_employee(payload)

        if http_method == "GET":
            # REST API: queryStringParameters
            # HTTP API: rawQueryString or queryStringParameters
            qs = event.get("queryStringParameters") or {}
            emp_id = qs.get("emp_id")
            if not emp_id and "rawQueryString" in event:
                # tiny fallback parser for rawQueryString like "emp_id=E001"
                for part in event["rawQueryString"].split("&"):
                    k, _, v = part.partition("=")
                    if k == "emp_id":
                        emp_id = v
                        break

            if not emp_id:
                return _build_response(400, {"error": "emp_id query parameter is required"})

            return handle_get_employee(emp_id)

    return _build_response(404, {"error": "Not found"})
