import cherrypy


def disable_json(input: bool = False, output: bool = False):
    """JSON input/output is enabled by default; disable it for specific handlers."""
    def decorator(func):
        if not hasattr(func, '_cp_config'):
            func._cp_config = {}
        if input:
            func._cp_config['tools.json_in.on'] = False
        if output:
            func._cp_config['tools.json_out.on'] = False
        return func
    return decorator
