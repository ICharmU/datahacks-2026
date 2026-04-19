from rest_framework.response import Response


def api_ok(data=None, message="ok", status_code=200):
    return Response(
        {
            "ok": True,
            "message": message,
            "data": data,
        },
        status=status_code,
    )


def api_error(message="error", errors=None, status_code=400):
    return Response(
        {
            "ok": False,
            "message": message,
            "errors": errors or {},
        },
        status=status_code,
    )